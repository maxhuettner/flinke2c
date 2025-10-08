# Streaming TCP Harness

Basic instructions for working with the harness.

## Build

- Compile the project in release mode: `cargo build --release`
- The resulting binary lives in `target/release/` (named after the crate in `Cargo.toml`).

## Run

- Execute the release binary to start the harness. Append `--help` to list all available subcommands and flags, for example:
  - `./target/release/<crate-name> --help`
  - or `cargo run --release -- --help`

## Logs

- Runtime logs are written to the `log/` directory. Re-running the harness with the same experiment name clears the previous logs automatically.
- Pass `--event-logging` to capture per-event throughput at both source and sink. Use the recorded event counts together with `(last_sink_ts - first_source_ts)` to compute aggregate throughput and runtime as needed.
