//! Assorted helpers used across babelfix.
//!
//! FIX UTC timestamp formatting ([`time_now_fix`] and [`fix_time`], producing the
//! `YYYYMMDD-HH:MM:SS.sss` form used in `SendingTime`/`TransactTime`), plus
//! [`wrap_and_report`] and [`wrap_and_bail`] for logging errors out of spawned
//! background tasks.

use futures::prelude::*;
use tracing::error;

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

pub fn time_now_fix() -> String {
  // Use chrono to get the current time in UTC and format it as per FIX standard
  let now = chrono::Utc::now();
  fix_time(now)
}

pub fn fix_time(when: chrono::DateTime<chrono::Utc>) -> String {
  when.format("%Y%m%d-%H:%M:%S.%3f").to_string()
}
