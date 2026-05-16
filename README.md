# bgpreader-compatible endpoint for bgpkit-broker

This is a fork of [bgpkit/bgpkit-broker](https://github.com/bgpkit/bgpkit-broker) (v0.11.0) by
[BGPKIT](https://bgpkit.com), used under the [MIT License](LICENSE).
The fork adds a BGPStream-compatible `/data` endpoint so that `bgpreader` can use a
self-hosted bgpkit-broker instance as a drop-in replacement for the CAIDA BGPStream broker.

## Motivation

[BGPStream](https://bgpstream.caida.org/) ships with a broker component
(`broker.bgpstream.caida.org`) that indexes public MRT archives from RouteViews and RIPE RIS.
`bgpreader`, the BGPStream command-line tool, queries this broker to discover which files to
download for a given time window and collector.

Unfortunately the CAIDA broker has become unreliable for recent data. As of early 2026 it does not work
for RIPE RIS collectors (e.g. `rrc00`), making it impossible to
work with RIPE RIS data without a workaround.

`bgpkit-broker` is a strong alternative:

- **Actively maintained**: Regular releases, used in production by Cloudflare
- **Self-hosted**: Run it on your own hardware with Docker, no external dependency
- **Auto-updating**: Crawls RouteViews and RIPE RIS in the background and keeps its index
  current in near real-time
- **Complete history**:  The database cover all archive files and is fast to start thanks to a bootstrapping mechanism

This fork extends bgpkit-broker with the BGPStream wire-format `/data` endpoint, so
existing `bgpreader` workflows require only a single flag change to point at the local instance.

## Quickstart

### Start the broker

**From source**: Run directly with Cargo:

```bash
# With an existing database (no live crawling):
cargo run --release --features cli -- serve data.db --no-update

# Bootstrap from scratch (downloads the latest snapshot then keeps itself updated):
cargo run --release --features cli -- serve data.db --bootstrap --silent
```

The broker listens on `http://localhost:40064` by default.

**Via Docker:**

```bash
docker compose up
```

### Query the broker directly

```bash
# Search for update files around 2010-09-01 00:00 UTC
curl "http://localhost:40064/search?ts_start=2010-09-01T00:00:00Z&ts_end=2010-09-01T01:00:00Z&collector_id=route-views.wide&data_type=updates" | jq .

# BGPStream-compatible endpoint (used by bgpreader internally)
curl "http://localhost:40064/data?types[]=updates&collectors[]=route-views.wide&intervals[]=1283299200,1283302800" | jq .
```

### Point `bgpreader` at the custom broker

Add `-o url=http://localhost:40064` to any `bgpreader` invocation:

```bash
# Default (CAIDA broker)
bgpreader -d broker \
  -w '1283299200,1283302800' \
  -c route-views.wide -t updates -m

# Local broker, drop-in replacement
bgpreader -d broker \
  -o 'url=http://localhost:40064' \
  -w '1283299200,1283302800' \
  -c route-views.wide -t updates -m

# We also host it for demo purpose
bgpreader -d broker \
  -o 'url=http://sandbox.ihr.live:40064' \
  -w '1283299200,1283302800' \
  -c route-views.wide -t updates -m
```

## Testing

Two integration test suites verify correctness against the CAIDA broker:

**[`tests/bgpreader_compat.rs`](tests/bgpreader_compat.rs)** — Runs `bgpreader` against both the local and CAIDA brokers, asserts identical element counts. Covers single/multiple collectors, single/multiple data types, and multi-chunk time windows (exercising bgpreader's internal 2-hour pagination via `minInitialTime`).

**[`tests/stream_consistency.rs`](tests/stream_consistency.rs)** — Parses the full bgpreader output from the local broker and verifies that every record has an expected collector, expected type (rib/update), and a timestamp within the query window. Asserts exact per-collector per-type element counts.

```bash
# Run all tests (broker must be running on port 40064, internet required for parity tests):
cargo test --test bgpreader_compat -- --nocapture
cargo test --test stream_consistency -- --nocapture
```
