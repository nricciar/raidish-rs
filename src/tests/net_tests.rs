use super::*;
use std::sync::{Arc, Mutex};

/// Fails `errors` times (as out of file descriptors would), then hands out
/// connections numbered from 0, forever.
struct Flaky {
    errors: usize,
    next: u32,
}

impl Accept for Flaky {
    type Conn = u32;
    async fn accept(&mut self) -> std::io::Result<(u32, SocketAddr)> {
        if self.errors > 0 {
            self.errors -= 1;
            return Err(std::io::Error::from_raw_os_error(24)); // EMFILE
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        self.next += 1;
        Ok((self.next - 1, "127.0.0.1:1".parse().unwrap()))
    }
}

/// Errors in a row pause longer and longer (no busy loop) and never end
/// the loop: once accepts work again, connections are served.
#[tokio::test(start_paused = true)]
async fn failed_accepts_back_off_and_dont_stop_the_loop() {
    let served = Arc::new(Mutex::new(Vec::new()));
    let s = served.clone();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(accepting(
        Flaky { errors: 8, next: 0 },
        "test",
        move |c, _| {
            let s = s.clone();
            async move { s.lock().unwrap().push((c, tokio::time::Instant::now())) }
        },
    ));
    while served.lock().unwrap().len() < 3 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    task.abort();
    let served = served.lock().unwrap();
    assert_eq!(
        served.iter().map(|(c, _)| *c).collect::<Vec<_>>()[..3],
        [0, 1, 2]
    );
    // 50+100+200+400+800 ms, then 1 s three times: 4.55 s before the first.
    let waited = served[0].1 - start;
    let want: Duration = (0..8)
        .map(|i| (MIN_PAUSE * 2u32.pow(i)).min(MAX_PAUSE))
        .sum();
    assert!(
        waited >= want,
        "waited {:?}, want at least {:?}",
        waited,
        want
    );
    assert!(
        waited < want + Duration::from_millis(100),
        "waited {:?}",
        waited
    );
}

/// A message repeated within the period is held back (and counted), then
/// said again with the count; different keys don't hold each other back.
#[test]
fn log_throttle_holds_repeats_back_and_counts_them() {
    let t = LogThrottle::new(Duration::from_millis(100));
    assert_eq!(t.allow("vol"), Some(0));
    assert_eq!(t.allow("vol"), None);
    assert_eq!(t.allow("vol"), None);
    assert_eq!(t.allow("other"), Some(0));
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(t.allow("vol"), Some(2));
    assert_eq!(t.allow("vol"), None);
}

/// The first answer wins; a refusal stops the asking (the others would
/// refuse too); with no answer at all, each endpoint's reason.
#[tokio::test]
async fn try_each_asks_in_turn_until_one_answers() {
    let asked = Mutex::new(Vec::new());
    let ask = |ep: &'static str| {
        asked.lock().unwrap().push(ep);
        async move {
            match ep {
                "down" => Answer::Unreachable("refused".to_string()),
                "no" => Answer::Refused("not allowed"),
                _ => Answer::Got(ep.len()),
            }
        }
    };
    assert_eq!(try_each(["down", "ok", "no"], ask).await.unwrap(), 2);
    assert_eq!(*asked.lock().unwrap(), ["down", "ok"]);

    assert!(matches!(
        try_each(["down", "no", "ok"], ask).await,
        Err(Tried::Refused("not allowed"))
    ));
    let e = try_each(["down", "down"], ask).await.unwrap_err();
    assert_eq!(e.to_string(), "down: refused; down: refused");
    let none: [&str; 0] = [];
    assert_eq!(
        try_each(none, ask).await.unwrap_err().to_string(),
        "no cluster endpoints"
    );
}
