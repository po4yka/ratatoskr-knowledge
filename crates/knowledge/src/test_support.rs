//! Disposable database support for integration tests.

use sqlx::Executor as _;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

use crate::{Database, PersistenceError};

/// Temporary Knowledge-owned blob root.
#[derive(Debug)]
pub struct TemporaryBlobRoot {
    path: std::path::PathBuf,
}

/// An isolated disposable Knowledge database.
#[derive(Debug)]
pub struct TestDatabase {
    /// Connected Knowledge database.
    pub database: Database,
    name: String,
}

impl TestDatabase {
    /// Creates an empty isolated database.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when database creation or connection fails.
    pub async fn create() -> Result<Self, PersistenceError> {
        let name = format!("knowledge_test_{}", Uuid::now_v7().simple());
        let admin_url = admin_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .map_err(PersistenceError::Connect)?;
        admin
            .execute(format!(r#"create database "{name}""#).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;

        let options = admin_url
            .parse::<PgConnectOptions>()
            .map_err(PersistenceError::Connect)?
            .database(&name);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .map_err(PersistenceError::Connect)?;
        let database = Database::from_pool(pool);
        database.apply_schema().await?;
        Ok(Self { database, name })
    }

    /// Closes and drops the database.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when cleanup fails.
    pub async fn cleanup(self) -> Result<(), PersistenceError> {
        self.database.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url())
            .await
            .map_err(PersistenceError::Connect)?;
        admin
            .execute(format!(r#"drop database if exists "{}" with (force)"#, self.name).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;
        Ok(())
    }
}

impl TemporaryBlobRoot {
    /// Creates a unique empty blob root.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the root cannot be created.
    pub async fn create() -> Result<Self, std::io::Error> {
        let path = std::env::temp_dir().join(format!("ratatoskr-knowledge-{}", Uuid::now_v7()));
        tokio::fs::create_dir_all(&path).await?;
        Ok(Self { path })
    }

    /// Returns the root path.
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TemporaryBlobRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.path);
    }
}

fn admin_url() -> String {
    match std::env::var("KNOWLEDGE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => "postgres://extractor:extractor@127.0.0.1:5434/extractor".to_owned(),
    }
}
