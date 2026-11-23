use super::*;

fn sh(script: &str) -> Command {
    let mut c = Command::new("sh");
    c.args(["-c", script]);
    c
}

/// Its output when it works; the command and its stderr when it
/// doesn't; and a tool that isn't there says so.
#[tokio::test]
async fn run_says_what_the_tool_said() {
    assert_eq!(run(sh("echo ' hi '")).await.unwrap(), "hi");
    assert_eq!(
        run(sh("echo oops >&2; exit 3")).await.unwrap_err(),
        "sh -c echo oops >&2; exit 3: oops"
    );
    let r = ran(sh("exit 2")).await.unwrap();
    assert_eq!((r.code, r.ok()), (Some(2), false));
    let e = run(Command::new("no-such-tool-raidish")).await.unwrap_err();
    assert!(e.starts_with("no-such-tool-raidish: "), "{}", e);
}
