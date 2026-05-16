/// Integration tests that verify the semantic consistency of bgpreader output
/// against the local broker.
///
/// Unlike `bgpreader_compat.rs` which only compares line counts between local
/// and CAIDA brokers, these tests parse bgpreader output and verify:
///   - Each record has an expected collector
///   - Each record has the expected type (rib/update)
///   - Timestamps fall within the queried time window
///   - Per-collector per-type element counts match expected values
///
/// Prerequisites:
///   - `bgpreader` in PATH
///   - a local bgpkit-broker instance running on http://localhost:40064
///     (start with: `cargo run --release --features cli -- serve data.db --no-update`)
///
/// Run with:
///   cargo test --test stream_consistency -- --nocapture
use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::time::Duration;

const LOCAL_BROKER_URL: &str = "http://localhost:40064";

// 2010-09-01 00:00:00 UTC .. 2010-09-01 01:55:00 UTC
const WIN_START: &str = "1283299200";
const WIN_END: &str = "1283306100";
const WIN_START_F: f64 = 1283299200.0;
const WIN_END_F: f64 = 1283306100.0;

// ── Parsed record ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RecordType {
    Rib,
    Update,
}

struct ParsedRecord {
    record_type: RecordType,
    collector: String,
    timestamp: f64,
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Run bgpreader against the local broker and return parsed records.
/// Skips RIB-begin marker lines (subtype "B").
fn run_bgpreader(
    time_window: (&str, &str),
    collectors: &[&str],
    record_types: &[&str],
    timeout: Duration,
) -> Vec<ParsedRecord> {
    let window_arg = format!("{},{}", time_window.0, time_window.1);
    let secs = timeout.as_secs().to_string();

    let mut cmd = Command::new("timeout");
    cmd.arg(&secs).arg("bgpreader").args(["-d", "broker"]);
    cmd.args(["-o", &format!("url={LOCAL_BROKER_URL}")]);
    cmd.args(["-w", &window_arg]);
    for c in collectors {
        cmd.args(["-c", c]);
    }
    for t in record_types {
        cmd.args(["-t", t]);
    }

    let output = cmd.output().expect("failed to spawn bgpreader");
    assert!(
        output.status.success(),
        "bgpreader failed or timed out (exit code: {:?})",
        output.status.code()
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut records = Vec::new();

    for line in stdout.lines() {
        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() < 5 {
            continue;
        }

        // Skip RIB-begin markers (R|B|...)
        if fields[1] == "B" {
            continue;
        }

        let record_type = match fields[0] {
            "R" => RecordType::Rib,
            "U" => RecordType::Update,
            _ => continue,
        };

        let timestamp: f64 = match fields[2].parse() {
            Ok(t) => t,
            Err(_) => continue,
        };

        records.push(ParsedRecord {
            record_type,
            collector: fields[4].to_string(),
            timestamp,
        });
    }

    records
}

/// Verify the stream output is consistent with the query parameters and expected counts.
fn check_stream(
    records: &[ParsedRecord],
    expected_collectors: &[&str],
    expected_types: &[RecordType],
    time_window: (f64, f64),
    expected_counts: &HashMap<(&str, RecordType), usize>,
) {
    let allowed_collectors: HashSet<&str> = expected_collectors.iter().copied().collect();
    let allowed_types: HashSet<RecordType> = expected_types.iter().copied().collect();

    let mut seen_collectors: HashSet<&str> = HashSet::new();
    let mut seen_types: HashSet<RecordType> = HashSet::new();
    let mut counts: HashMap<(&str, RecordType), usize> = HashMap::new();

    for record in records {
        // Check collector is in the expected set
        assert!(
            allowed_collectors.contains(record.collector.as_str()),
            "unexpected collector '{}' in output",
            record.collector
        );

        // Check record type is expected
        assert!(
            allowed_types.contains(&record.record_type),
            "unexpected record type {:?} in output",
            record.record_type
        );

        // Check timestamp is within the query window
        assert!(
            record.timestamp >= time_window.0 && record.timestamp <= time_window.1,
            "timestamp {} outside query window [{}, {}]",
            record.timestamp,
            time_window.0,
            time_window.1
        );

        seen_collectors.insert(
            allowed_collectors
                .iter()
                .find(|&&c| c == record.collector)
                .unwrap(),
        );
        seen_types.insert(record.record_type);

        *counts
            .entry((
                allowed_collectors
                    .iter()
                    .find(|&&c| c == record.collector)
                    .unwrap(),
                record.record_type,
            ))
            .or_insert(0) += 1;
    }

    // All expected collectors should appear
    for c in expected_collectors {
        assert!(
            seen_collectors.contains(c),
            "expected collector '{c}' not seen in output"
        );
    }

    // All expected types should appear
    for t in expected_types {
        assert!(
            seen_types.contains(t),
            "expected record type {t:?} not seen in output"
        );
    }

    // Check per-collector per-type counts
    for ((collector, rtype), expected) in expected_counts {
        let actual = counts.get(&(*collector, *rtype)).copied().unwrap_or(0);
        assert_eq!(
            actual, *expected,
            "count mismatch for ({collector}, {rtype:?}): got {actual}, expected {expected}"
        );
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[test]
fn stream_updates() {
    let records = run_bgpreader(
        (WIN_START, WIN_END),
        &["route-views.wide", "route-views.sydney"],
        &["updates"],
        Duration::from_secs(180),
    );

    let expected_counts = HashMap::from([
        (("route-views.sydney", RecordType::Update), 48287),
        (("route-views.wide", RecordType::Update), 29491),
    ]);

    check_stream(
        &records,
        &["route-views.wide", "route-views.sydney"],
        &[RecordType::Update],
        (WIN_START_F, WIN_END_F),
        &expected_counts,
    );
}

#[test]
fn stream_ribs() {
    let records = run_bgpreader(
        (WIN_START, WIN_END),
        &["route-views.wide", "route-views.sydney"],
        &["ribs"],
        Duration::from_secs(900),
    );

    let expected_counts = HashMap::from([
        (("route-views.sydney", RecordType::Rib), 828938),
        (("route-views.wide", RecordType::Rib), 990165),
    ]);

    check_stream(
        &records,
        &["route-views.wide", "route-views.sydney"],
        &[RecordType::Rib],
        (WIN_START_F, WIN_END_F),
        &expected_counts,
    );
}

#[test]
fn stream_both() {
    let records = run_bgpreader(
        (WIN_START, WIN_END),
        &["route-views.wide", "route-views.sydney"],
        &["updates", "ribs"],
        Duration::from_secs(900),
    );

    let expected_counts = HashMap::from([
        (("route-views.sydney", RecordType::Rib), 828938),
        (("route-views.wide", RecordType::Rib), 990165),
        (("route-views.sydney", RecordType::Update), 48287),
        (("route-views.wide", RecordType::Update), 29491),
    ]);

    check_stream(
        &records,
        &["route-views.wide", "route-views.sydney"],
        &[RecordType::Rib, RecordType::Update],
        (WIN_START_F, WIN_END_F),
        &expected_counts,
    );
}
