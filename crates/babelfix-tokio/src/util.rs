//! Assorted helpers used across babelfix.
//!
//! The wall clock lives here rather than in `babelfix-core`: the core formats a
//! timestamp it is handed, and this crate is the one that decides what time it
//! is. See [`babelfix_core::time`] for the formatting itself.

use futures::prelude::*;
use tracing::error;

pub use babelfix_core::time::{FixTime, TimePrecision, fix_time};

pub async fn wrap_and_report<F, T>(future: F) -> Option<T>
where
  F: Future<Output = Result<T, crate::Error>> + Send + 'static,
{
  match future.await {
    Ok(result) => Some(result),
    Err(e) => {
      error!("An error occurred: {e}");
      None
    }
  }
}

/// The current UTC time, formatted for `SendingTime`/`TransactTime`.
pub fn time_now_fix() -> String {
  fix_time(chrono::Utc::now(), TimePrecision::Millis).into()
}
