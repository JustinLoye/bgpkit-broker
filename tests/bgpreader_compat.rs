/// Integration tests that run `bgpreader` as a subprocess and compare line counts
/// between the local broker and the CAIDA broker.
///
/// Each test calls bgpreader twice — once against the local broker and once against
/// CAIDA — and asserts both return the same number of BGP elements. This validates
/// that the local broker is a correct drop-in replacement for CAIDA.
///
/// Prerequisites:
///   - `bgpreader` in PATH
///   - internet access (to reach broker.bgpstream.caida.org)
///   - a local bgpkit-broker instance running on http://localhost:40064
///     (start with: `cargo run --release --features cli -- serve data.db --no-update`)
///
/// Run with:
///   cargo test --test bgpreader_compat -- --nocapture
///
/// The single `#[ignore]` test documents a known CAIDA broker bug (it asserts a
/// timeout, not a count). Run it explicitly with:
///   cargo test --test bgpreader_compat -- --include-ignored --nocapture
use std::process::Command;
use std::time::Duration;

const LOCAL_BROKER_URL: &str = "http://localhost:40064";

// ── 2010 reference window (1h) ─────────────────────────────────────────────
// 2010-09-01 00:00:00 UTC .. 2010-09-01 01:00:00 UTC  (small, fast)
const WIN_2010_START: &str = "1283299200";
const WIN_2010_END: &str = "1283302800";

// ── 2010 4-hour window (updates) ──────────────────────────────────────────
// ±5 min offset straddles the 2-hour bgpreader chunk boundary, exercising the
// chunked-pagination path where multiple requests with advancing minInitialTime
// are issued.
const WIN_2010_4H_START: &str = "1283298900"; // 2010-09-01 ~00:00 UTC
const WIN_2010_4H_END: &str = "1283313900"; //   2010-09-01 ~04:00 UTC

// ── 2010 9-hour window (ribs) ─────────────────────────────────────────────
// Spans four 2-hour bgpreader chunks — exercises multi-chunk RIB pagination.
const WIN_2010_9H_START: &str = "1283298900"; // 2010-09-01 ~00:00 UTC
const WIN_2010_9H_END: &str = "1283331900"; //   2010-09-01 ~09:00 UTC

// ── Feb 2026 window (CAIDA-bug) ────────────────────────────────────────────
// 2026-02-01 00:00:00 UTC .. 2026-02-01 00:15:00 UTC
// CAIDA broker hangs for RIPE RIS collectors in this window; local broker works.
const WIN_FEB2026_START: &str = "1769904000";
const WIN_FEB2026_END: &str = "1769904900";

// ── Helper ─────────────────────────────────────────────────────────────────

/// Run bgpreader and return the number of output lines, or None if it timed out.
fn bgpreader_line_count(
    broker_url: Option<&str>,
    time_window: (&str, &str),
    collector: &str,
    record_type: &str,
    timeout: Duration,
) -> Option<usize> {
    let window_arg = format!("{},{}", time_window.0, time_window.1);
    let secs = timeout.as_secs().to_string();

    let mut cmd = Command::new("timeout");
    cmd.arg(&secs).arg("bgpreader").args(["-d", "broker"]);
    if let Some(url) = broker_url {
        cmd.args(["-o", &format!("url={url}")]);
    }
    cmd.args(["-w", &window_arg, "-c", collector, "-t", record_type, "-m"]);

    let output = cmd.output().expect("failed to spawn bgpreader");

    // `timeout` exits with 124 when it kills the child process
    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        if code == 124 || code == 143 {
            eprintln!(
                "bgpreader timed out after {secs}s (broker={broker_url:?}, collector={collector}, type={record_type})"
            );
        }
        return None;
    }

    Some(output.stdout.iter().filter(|&&b| b == b'\n').count())
}

/// Like `bgpreader_line_count` but with multiple collectors (multiple -c flags).
fn bgpreader_multi_collector_line_count(
    broker_url: Option<&str>,
    time_window: (&str, &str),
    collectors: &[&str],
    record_type: &str,
    timeout: Duration,
) -> Option<usize> {
    let window_arg = format!("{},{}", time_window.0, time_window.1);
    let secs = timeout.as_secs().to_string();

    let mut cmd = Command::new("timeout");
    cmd.arg(&secs).arg("bgpreader").args(["-d", "broker"]);
    if let Some(url) = broker_url {
        cmd.args(["-o", &format!("url={url}")]);
    }
    cmd.args(["-w", &window_arg]);
    for c in collectors {
        cmd.args(["-c", c]);
    }
    cmd.args(["-t", record_type, "-m"]);

    let output = cmd.output().expect("failed to spawn bgpreader");

    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        if code == 124 || code == 143 {
            eprintln!(
                "bgpreader timed out after {secs}s (broker={broker_url:?}, collectors={collectors:?}, type={record_type})"
            );
        }
        return None;
    }

    Some(output.stdout.iter().filter(|&&b| b == b'\n').count())
}

/// Like `bgpreader_line_count` but with multiple collectors and types.
fn bgpreader_multi_type_line_count(
    broker_url: Option<&str>,
    time_window: (&str, &str),
    collectors: &[&str],
    record_types: &[&str],
    timeout: Duration,
) -> Option<usize> {
    let window_arg = format!("{},{}", time_window.0, time_window.1);
    let secs = timeout.as_secs().to_string();

    let mut cmd = Command::new("timeout");
    cmd.arg(&secs).arg("bgpreader").args(["-d", "broker"]);
    if let Some(url) = broker_url {
        cmd.args(["-o", &format!("url={url}")]);
    }
    cmd.args(["-w", &window_arg]);
    for c in collectors {
        cmd.args(["-c", c]);
    }
    for t in record_types {
        cmd.args(["-t", t]);
    }
    cmd.arg("-m");

    let output = cmd.output().expect("failed to spawn bgpreader");

    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        if code == 124 || code == 143 {
            eprintln!(
                "bgpreader timed out after {secs}s (broker={broker_url:?}, collectors={collectors:?}, types={record_types:?})"
            );
        }
        return None;
    }

    Some(output.stdout.iter().filter(|&&b| b == b'\n').count())
}

/// Run bgpreader against both the local broker and CAIDA, assert they return the
/// same number of BGP elements, and return that shared count.
fn assert_parity(
    time_window: (&str, &str),
    collector: &str,
    record_type: &str,
    timeout: Duration,
) -> usize {
    let local = bgpreader_line_count(
        Some(LOCAL_BROKER_URL),
        time_window,
        collector,
        record_type,
        timeout,
    )
    .unwrap_or_else(|| {
        panic!("local broker timed out (collector={collector}, type={record_type})")
    });

    let caida = bgpreader_line_count(None, time_window, collector, record_type, timeout)
        .unwrap_or_else(|| {
            panic!("CAIDA broker timed out (collector={collector}, type={record_type})")
        });

    println!("{collector} {record_type} — local: {local}, CAIDA: {caida}");
    assert_eq!(
        local, caida,
        "local and CAIDA broker returned different element counts for {collector} {record_type}"
    );
    local
}

// ── Parity tests: 1-hour window (2010) ────────────────────────────────────

#[test]
fn parity_updates_2010_route_views_wide() {
    assert_parity(
        (WIN_2010_START, WIN_2010_END),
        "route-views.wide",
        "updates",
        Duration::from_secs(120),
    );
}

#[test]
fn parity_ribs_2010_route_views_sydney() {
    assert_parity(
        (WIN_2010_START, WIN_2010_END),
        "route-views.sydney",
        "ribs",
        Duration::from_secs(600),
    );
}

// ── Parity test: multiple collectors in a single query ────────────────────

#[test]
fn parity_updates_2010_multi_collector() {
    // bgpreader sends collectors[]=a&collectors[]=b when given multiple -c flags.
    // This exercises the repeated-key query string parsing.
    let window = (WIN_2010_START, WIN_2010_END);
    let timeout = Duration::from_secs(180);

    let local = bgpreader_multi_collector_line_count(
        Some(LOCAL_BROKER_URL),
        window,
        &["route-views.wide", "rrc04"],
        "updates",
        timeout,
    )
    .expect("local broker timed out for multi-collector query");

    let caida = bgpreader_multi_collector_line_count(
        None,
        window,
        &["route-views.wide", "rrc04"],
        "updates",
        timeout,
    )
    .expect("CAIDA broker timed out for multi-collector query");

    println!("multi-collector updates — local: {local}, CAIDA: {caida}");
    assert_eq!(
        local, caida,
        "local and CAIDA broker returned different counts for multi-collector query"
    );
}

// ── Parity test: both ribs and updates simultaneously ─────────────────────

#[test]
fn parity_both_types_2010_route_views_wide() {
    // bgpreader sends types[]=updates&types[]=ribs when given -t updates -t ribs.
    // Window around 08:00 UTC where Route Views has both a RIB dump and updates.
    let window = ("1283327940", "1283328060"); // 2010-09-01 ~07:59–08:01 UTC
    let timeout = Duration::from_secs(600);

    let local = bgpreader_multi_type_line_count(
        Some(LOCAL_BROKER_URL),
        window,
        &["route-views.wide", "rrc04"],
        &["updates", "ribs"],
        timeout,
    )
    .expect("local broker timed out for multi-type query");

    let caida = bgpreader_multi_type_line_count(
        None,
        window,
        &["route-views.wide", "rrc04"],
        &["updates", "ribs"],
        timeout,
    )
    .expect("CAIDA broker timed out for multi-type query");

    println!("multi-type (ribs+updates) — local: {local}, CAIDA: {caida}");
    assert_eq!(
        local, caida,
        "local and CAIDA broker returned different counts for multi-type query"
    );
}

// ── Parity tests: multi-chunk windows (2010) ──────────────────────────────
//
// bgpreader internally splits long time windows into 2-hour chunks and issues
// one broker request per chunk with an advancing `minInitialTime` parameter.
// These tests verify that our minInitialTime post-filter handles multiple
// sequential chunk requests correctly and matches CAIDA's output exactly.

#[test]
fn parity_updates_4h_route_views_wide() {
    // 4-hour window → two 2-hour bgpreader chunks (RouteViews)
    assert_parity(
        (WIN_2010_4H_START, WIN_2010_4H_END),
        "route-views.wide",
        "updates",
        Duration::from_secs(300),
    );
}

#[test]
fn parity_updates_4h_rrc04() {
    // 4-hour window → two 2-hour bgpreader chunks (RIPE RIS)
    assert_parity(
        (WIN_2010_4H_START, WIN_2010_4H_END),
        "rrc04",
        "updates",
        Duration::from_secs(300),
    );
}

#[test]
fn parity_ribs_9h_route_views_wide() {
    // 9-hour window → four 2-hour bgpreader chunks (RouteViews)
    assert_parity(
        (WIN_2010_9H_START, WIN_2010_9H_END),
        "route-views.wide",
        "ribs",
        Duration::from_secs(900),
    );
}

#[test]
fn parity_ribs_9h_rrc04() {
    // 9-hour window → four 2-hour bgpreader chunks (RIPE RIS)
    assert_parity(
        (WIN_2010_9H_START, WIN_2010_9H_END),
        "rrc04",
        "ribs",
        Duration::from_secs(900),
    );
}

// ── Feb 2026: CAIDA broker is broken for RIPE RIS ─────────────────────────
//
// CAIDA broker hangs (returns no data) for rrc00 in the Feb 2026 window due to
// a known indexing gap. A parity test cannot be written here. Instead we assert:
//   1. our local broker returns substantial data (>100k elements)
//   2. [#ignore] the CAIDA broker times out, documenting the known bug

#[test]
fn local_broker_rrc00_updates_feb2026_returns_data() {
    let count = bgpreader_line_count(
        Some(LOCAL_BROKER_URL),
        (WIN_FEB2026_START, WIN_FEB2026_END),
        "rrc00",
        "updates",
        Duration::from_secs(120),
    )
    .expect("local broker timed out for rrc00 updates Feb 2026");
    println!("rrc00 updates Feb 2026 (local): {count} lines");
    assert!(
        count > 100_000,
        "expected >100k BGP elements from local broker for rrc00 Feb 2026, got {count}"
    );
}

/// Documents a known CAIDA broker bug: rrc00 updates for the Feb 2026 window
/// hang indefinitely. Kept `#[ignore]` because it intentionally waits for a
/// timeout, which is slow and validates an external system's broken behaviour
/// rather than our own broker's correctness.
///
/// If CAIDA ever fixes this, this test will fail — that's the desired signal.
#[test]
#[ignore]
fn caida_broker_rrc00_updates_feb2026_is_broken() {
    let count = bgpreader_line_count(
        None, // CAIDA broker
        (WIN_FEB2026_START, WIN_FEB2026_END),
        "rrc00",
        "updates",
        Duration::from_secs(30), // intentionally short — CAIDA hangs here
    );
    assert!(
        count.is_none(),
        "CAIDA broker unexpectedly returned {n} lines for rrc00 Feb 2026 — \
         the known bug may be fixed; consider removing this test",
        n = count.unwrap_or(0)
    );
    println!("Confirmed: CAIDA broker timed out for rrc00 Feb 2026 (known bug)");
}
