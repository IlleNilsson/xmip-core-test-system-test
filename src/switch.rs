//! Whether this test process may reach the internet (ADR-0045): the one
//! variable, `XMIP_ONLINE`, and the words it reads.
//!
//! It is the test process's own, never a node's. Whether a node may assume
//! the internet is the Node capability `online` its cluster's file declares
//! for it, as every other Node capability is (ADR-0056, amendment
//! 2026-11-24), read with its roles into its [`Capability`](node::Capability).
//!
//! False unless set. Nothing in the estate reaches out at runtime, so the
//! default is that no test may; the switch exists so the first test that
//! genuinely needs the internet reads it and stays silent without it, rather
//! than bringing the suite online with it. `XMIP_ONLINE=true` is how an
//! operator says the world is there.

/// Whether this process may assume the internet: `XMIP_ONLINE=true`.
#[must_use]
pub fn online() -> bool {
    std::env::var("XMIP_ONLINE").is_ok_and(|raw| parse(&raw) == Some(true))
}

/// The switch a word means: `true`, `false`, `yes`, `no`, `on`, `off`.
#[must_use]
pub fn parse(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Some(true),
        "false" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// For a test that needs the internet: `true` when it may run, and when it
/// may not, the line to print in its place so a reader sees it was skipped
/// on purpose, not lost.
#[must_use]
pub fn needs_internet(what: &str) -> bool {
    if online() {
        return true;
    }
    println!("skipped offline: {what} needs the internet; set XMIP_ONLINE=true to run it");
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_is_off_unless_said_and_reads_the_usual_words() {
        assert_eq!(parse("TRUE"), Some(true));
        assert_eq!(parse("off"), Some(false));
        assert_eq!(parse("maybe"), None);
    }

    #[test]
    fn a_test_that_needs_the_internet_reads_the_switch() {
        // The suite runs offline; this proves the gate says so and yields.
        if needs_internet("this very test") {
            assert!(online());
        } else {
            assert!(!online());
        }
    }
}
