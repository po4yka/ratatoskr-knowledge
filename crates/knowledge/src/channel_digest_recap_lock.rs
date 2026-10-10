//! Per-run session advisory lock that keeps one executor on a recap run.

use sqlx::{Connection as _, PgConnection};

use crate::{ChannelRecapPipelineError, Database, PersistenceError};

const LOCK: &str = "select pg_try_advisory_lock(hashtextextended($1::text, 0))";
const UNLOCK: &str = "select pg_advisory_unlock(hashtextextended($1::text, 0))";

/// A session advisory lock on one recap run, held on a connection dedicated to it.
///
/// The connection is detached from the pool, so a lock can never be handed to another borrower.
/// Dropping the guard without [`RunLock::release`] (for instance when the executing future is
/// cancelled) drops the connection, which ends the session and releases the lock on the server.
#[derive(Debug)]
pub(crate) struct RunLock {
    connection: PgConnection,
    key: String,
}

impl RunLock {
    /// Takes the lock for `recap_run_id`.
    ///
    /// # Errors
    ///
    /// [`ChannelRecapPipelineError::State`] when another executor holds the run, and a
    /// persistence error when the database cannot be reached.
    pub(crate) async fn acquire(
        database: &Database,
        recap_run_id: uuid::Uuid,
    ) -> Result<Self, ChannelRecapPipelineError> {
        let mut connection = database
            .pool()
            .acquire()
            .await
            .map_err(PersistenceError::Query)?
            .detach();
        let key = recap_run_id.to_string();
        let acquired: bool = sqlx::query_scalar(LOCK)
            .bind(&key)
            .fetch_one(&mut connection)
            .await
            .map_err(PersistenceError::Query)?;
        if acquired {
            Ok(Self { connection, key })
        } else {
            Err(ChannelRecapPipelineError::State)
        }
    }

    /// Unlocks the run and closes the dedicated connection.
    ///
    /// A connection that cannot confirm the unlock is closed anyway, which releases the lock.
    pub(crate) async fn release(mut self) {
        let _unlocked: Result<bool, sqlx::Error> = sqlx::query_scalar(UNLOCK)
            .bind(&self.key)
            .fetch_one(&mut self.connection)
            .await;
        let _closed = self.connection.close().await;
    }
}
