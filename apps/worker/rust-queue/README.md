# Experimental PostgreSQL queue primitives

This crate is an isolated starting point for a Rust/PostgreSQL rewrite. It is **not a Novu-compatible worker** and is not wired into the existing service. Do not deploy it as a replacement. The target is 100% compatibility with the selected Novu version, but this crate does not establish that compatibility.

The migration defines a queue table. The library exposes transactional enqueue (including delay), per-kind claim with `SKIP LOCKED`, lease renewal, token-fenced completion, failure and retry, pending-job cancellation scoped by environment, and explicit recovery of expired jobs. Claim and recovery policy are deliberately separate: automatically retrying every expired job could change Novu's delivery semantics or duplicate external side effects. The shared [`trigger_gate`](../../api/rust-contracts/src/trigger_gate.rs) module reproduces a small, explicitly bounded part of the stored-workflow trigger decisions from the reference source; it does not implement the full API or workflow engine.

An experimental worker loop supports only the single-step in-app prototype described in the [API README](../../api/rust-api/README.md). It atomically creates simple subscriber and message records and completes a job; it is not compatible with the Novu Inbox. The limited interpolation implementation can be checked against the reference `handlebars@4.7.10` by installing that package in a temporary directory and running `NODE_PATH=/path/to/node_modules cargo test --test reference_template -- --ignored`. There is no scheduler process, cancellation of running work, generic queue deduplication, automatic retry/backoff, tenant authentication, external provider, or integration with Novu. In particular, a lease cannot prevent an already-started external call from completing after lease expiry. Consumers must decide their side-effect and recovery policy. The migrations are initial development artifacts, not a production migration system; run them only on an isolated database.

Run static checks with `cargo test` and `cargo clippy --all-targets -- -D warnings`. To launch a disposable PostgreSQL 17 Docker container, run `./scripts/test-postgres.sh`; it binds a random loopback port and removes the container on exit.

Alternatively, the integration test deliberately **drops and recreates** `novu_queue_jobs`; run it only against a disposable PostgreSQL database:

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@localhost:5432/DISPOSABLE_DATABASE' \
  cargo test --test postgres -- --ignored
```

The integration test is ignored by default so an unset database is not mistaken for a successful database test. For architectural context and compatibility requirements see [the design record](../../../RUST_POSTGRES_DESIGN_KO.md).
