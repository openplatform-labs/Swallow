# Experimental Rust API

This is an incomplete, isolated implementation for comparison with Novu at commit `895388f37efe9e06de0aa333e5727303e6ad6853`. **It is not a production service or a drop-in Novu replacement.** The fixture schema is not a migration from Novu and neither an API-key management endpoint nor a workflow management endpoint exists.

`POST /v1/events/trigger` has a limited path for `Authorization: ApiKey …`, SHA-256 key lookup scoped to an environment with an associated-user existence check, recipient validation, and the missing, inactive and no-step stored-workflow outcomes. A fixture workflow with `in_app_content` supports one in-app message for string subscriber IDs: the API enqueues a job and the experimental Rust worker creates subscribers and messages in PostgreSQL. The message may contain simple Handlebars-style paths resolving to strings, booleans or missing values. Other active workflows and unsupported trigger options return 501. `processed` means the job was accepted, not that a message was delivered; the worker must run separately.

This is a narrow prototype, not a compatible Inbox: there is no message API, real-time delivery, full Handlebars support, preferences, integration selection, conditional steps, or handling of subscriber objects or topics. Template expressions with helpers, blocks, numbers, arrays and objects are rejected; subscriber profile fields other than `subscriberId` are unavailable, and branding is limited to the default color. JWT/OAuth, user membership and role-based permission checks, tenant overrides, validation error details, payload schemas, rate limits, other channels, API/SDK coverage, and many other behaviours are not implemented. Its transaction ledger retains keys indefinitely; this differs from Novu's tier-dependent retention and has not been verified against its duplicate-trigger semantics. The generated transaction ID has the same prefix and length but is not a MongoDB ObjectId. The prototype database connection uses `NoTls` and must not be exposed as a production configuration.

## Verification

- `cargo test --locked`: unit tests and a differential test which extracts the original TypeScript recipient-validation function from the current checkout, executes it with Node.js type stripping, and compares a bounded fixture set with the Rust function. This tests that function only, not the original HTTP server. Requires Node.js with `--experimental-strip-types` support.
- `./scripts/test-postgres.sh`: starts a disposable PostgreSQL 17 Docker container bound to a random loopback port, runs HTTP-router + database integration tests (including queue, worker, subscribers and static in-app messages), and removes the container. The test asserts selected results from original Novu source/e2e tests; it does not run the original server.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`.

The DB test drops and recreates its experimental queue, subscriber, message, transaction, workflow, and API key tables. If running it manually, use **only a disposable database**:

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@localhost:5432/DISPOSABLE_DATABASE' \
  cargo test --locked --test api_postgres -- --ignored
```

The API binary requires `DATABASE_URL` and defaults to listening on `127.0.0.1:3000` (`LISTEN_ADDR` overrides it). The worker can be started with `cargo run --manifest-path ../../worker/rust-queue/Cargo.toml --bin in-app-worker`, with the same `DATABASE_URL`. Neither creates schema or seeds data. Apply the experimental API migration and both queue migrations to an empty disposable database; these files do not provide versioned migration or upgrade handling. The worker exits on errors and does not automatically recover expired jobs.
