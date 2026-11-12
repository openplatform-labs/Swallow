//! Shared, incomplete contracts for the experimental Rust services.
pub mod email;
pub mod locale;
pub mod message;
pub mod postgres;
pub mod preference;
pub mod recipients;
pub mod socket;
pub mod template;
pub mod timezone;
pub mod trigger_gate;

/// Internal prototype job kind. This is not a Novu wire queue name.
pub const EXPERIMENTAL_IN_APP_JOB_KIND: &str = "experimental_static_in_app";
