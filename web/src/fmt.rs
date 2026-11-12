//! Number and time formatting.

use raidish::disk::BLOCK_SIZE;
use raidish::inspect::BlockKind;

pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{} B", n)
    } else if v >= 100.0 {
        format!("{:.0} {}", v, UNITS[u])
    } else {
        format!("{:.1} {}", v, UNITS[u])
    }
}

pub fn blocks(n: u64) -> String {
    bytes(n * BLOCK_SIZE as u64)
}

pub fn count(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

pub fn pct(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

/// "3 s ago", "5 min ago", ...
pub fn ago(then_ms: f64, now_ms: f64) -> String {
    let s = ((now_ms - then_ms) / 1000.0).max(0.0);
    if s < 60.0 {
        format!("{:.0} s ago", s)
    } else if s < 3600.0 {
        format!("{:.0} min ago", s / 60.0)
    } else if s < 86400.0 {
        format!("{:.1} h ago", s / 3600.0)
    } else {
        format!("{:.1} d ago", s / 86400.0)
    }
}

/// Local wall-clock time of a unix-ms timestamp.
pub fn clock(ms: u64) -> String {
    let d = js_sys::Date::new(&(ms as f64).into());
    format!(
        "{:02}:{:02}:{:02}",
        d.get_hours(),
        d.get_minutes(),
        d.get_seconds()
    )
}

pub fn ms(us: f64) -> String {
    if us < 1000.0 {
        format!("{:.0} µs", us)
    } else {
        format!("{:.1} ms", us / 1000.0)
    }
}

/// The map's colors, matching `raidish map` in the terminal.
pub fn kind_color(k: BlockKind) -> &'static str {
    match k {
        BlockKind::Superblock => "#e5484d",
        BlockKind::Uberblock => "#f76b15",
        BlockKind::SpaceMap => "#f5c518",
        BlockKind::FileIndex => "#3e9bf5",
        BlockKind::Inode => "#29d3e0",
        BlockKind::Data => "#30a46c",
        BlockKind::Other => "#6b8f71",
        BlockKind::Free => "var(--free)",
        BlockKind::Slack => "var(--slack)",
    }
}

/// A stable color per object, for "color by owner".
pub fn owner_color(owner: u32) -> String {
    let hue = (owner as f64 * 137.508) % 360.0;
    format!("hsl({:.0} 65% 55%)", hue)
}
