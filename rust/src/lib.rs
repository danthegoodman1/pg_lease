use std::{error::Error, future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use rand::Rng;
use sqlx::{Acquire, Pool, Postgres, pool::PoolConnection};
use tokio::{sync::Mutex, task::AbortHandle};
use tokio_util::sync::CancellationToken;

pub struct LeaseLooper<T, Fut>
where
    T: Fn() -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'static,
{
    lease_name: String,
    worker_id: String,
    looper_func: T,
    options: LeaseLooperOptions,
    pool: Pool<Postgres>,

    abort_handle: Arc<Mutex<Option<AbortHandle>>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseLooperOptions {
    pub loop_interval: Duration,
    pub loop_interval_jitter: Duration,
    pub lease_duration: Duration,
    pub lease_heartbeat_interval: Duration,
}

impl<T, Fut> LeaseLooper<T, Fut>
where
    T: Fn() -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'static,
{
    pub fn new(
        lease_name: String,
        looper_func: T,
        worker_id: String,
        pool: Pool<Postgres>,
        options: LeaseLooperOptions,
    ) -> Self {
        Self {
            lease_name,
            worker_id,
            looper_func,
            options,
            pool,
            abort_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut abort_handle = self.abort_handle.lock().await;

        let join_handle = tokio::task::spawn(Self::launch_looper(
            self.looper_func.clone(),
            self.pool.clone(),
            self.options,
            self.lease_name.clone(),
            self.worker_id.clone(),
        ));

        *abort_handle = Some(join_handle.abort_handle());

        Ok(())
    }

    async fn launch_looper(
        looper_func: T,
        pool: Pool<Postgres>,
        options: LeaseLooperOptions,
        lease_name: String,
        worker_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("launching looper, attempting to create table {}", worker_id);

        if let Err(e) = sqlx::query(
            r#"
            create table if not exists _pg_lease (
                name text,
                worker_id text,
                held_until timestamptz,
                primary key (name)
            )"#,
        )
        .execute(&pool)
        .await
        {
            eprintln!("Failed to create _pg_lease table for {}: {}", worker_id, e);
            return Err(format!("Failed to create _pg_lease table: {}", e).into());
        }

        println!("table created, attempting to acquire lease: {}", worker_id);

        loop {
            if let Err(e) =
                Self::wait_for_lease(&pool, &lease_name, worker_id.as_str(), &options).await
            {
                eprintln!("Error waiting for lease {}: {}", worker_id, e);
                tokio::time::sleep(options.loop_interval).await;
                continue;
            }

            let looper_handle = tokio::task::spawn(looper_func());

            let heartbeat_handle = tokio::task::spawn(Self::heartbeat_loop(
                looper_handle.abort_handle(),
                pool.clone(),
                lease_name.clone(),
                worker_id.clone(),
                options,
            ));

            let looper_join_result = looper_handle.await;

            heartbeat_handle.abort();

            match looper_join_result {
                Ok(Ok(())) => {
                    // looper_func returned successfully
                    println!("looper_func returned successfully, dropping lease");
                    if let Err(e) = Self::drop_lease(&pool, &lease_name, &worker_id).await {
                        eprintln!("Failed to drop lease for {}: {}", worker_id, e);
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    // looper_func returned an error
                    eprintln!("[ERR]: looper task failed: {:?}", e);
                    if let Err(drop_err) = Self::drop_lease(&pool, &lease_name, &worker_id).await {
                        eprintln!(
                            "Failed to drop lease after task failure for {}: {}",
                            worker_id, drop_err
                        );
                    }
                }
                Err(join_error) => {
                    // Task was cancelled or panicked
                    if join_error.is_cancelled() {
                        println!(
                            "looper task was cancelled (likely due to lost lease), continuing loop"
                        );
                        continue;
                    } else {
                        // Task panicked
                        eprintln!("[ERR]: looper task panicked: {:?}", join_error);
                        if let Err(drop_err) =
                            Self::drop_lease(&pool, &lease_name, &worker_id).await
                        {
                            eprintln!(
                                "Failed to drop lease after task panic for {}: {}",
                                worker_id, drop_err
                            );
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_lease(
        pool: &Pool<Postgres>,
        lease_name: &str,
        worker_id: &str,
        options: &LeaseLooperOptions,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("attempting to acquire lease: {}", worker_id);

        let sleep_duration = options.loop_interval
            + Duration::from_millis(
                rand::rng().random_range(0..options.loop_interval_jitter.as_millis()) as u64,
            );

        loop {
            match Self::try_acquire_lease(pool, lease_name, worker_id, options).await {
                Ok(true) => {
                    println!("successfully acquired lease: {}", worker_id);
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    eprintln!("Error trying to acquire lease for {}: {}", worker_id, e);
                    return Err(e);
                }
            }

            tokio::time::sleep(sleep_duration).await;
        }

        Ok(())
    }

    async fn try_acquire_lease(
        pool: &Pool<Postgres>,
        lease_name: &str,
        worker_id: &str,
        options: &LeaseLooperOptions,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let result: (String, DateTime<Utc>) = sqlx::query_as(
            r#"
            INSERT INTO _pg_lease (name, worker_id, held_until)
            VALUES ($1, $2, NOW() + $3::interval)
            ON CONFLICT (name) DO UPDATE SET
                worker_id = CASE
                    WHEN _pg_lease.held_until < NOW() THEN $2
                    ELSE _pg_lease.worker_id
                END,
                held_until = CASE
                    WHEN _pg_lease.held_until < NOW() THEN NOW() + $3::interval
                    ELSE _pg_lease.held_until
                END
            RETURNING worker_id, held_until"#,
        )
        .bind(lease_name)
        .bind(worker_id)
        .bind(options.lease_duration)
        .fetch_one(pool)
        .await?;

        if result.0 == worker_id {
            println!("successfully acquired lease: {}", worker_id);
            Ok(true)
        } else {
            println!("lease held by another worker: {}", result.0);
            Ok(false)
        }
    }

    async fn heartbeat_loop(
        abort_handle: AbortHandle,
        pool: Pool<Postgres>,
        lease_name: String,
        worker_id: String,
        options: LeaseLooperOptions,
    ) {
        loop {
            tokio::time::sleep(options.lease_heartbeat_interval).await;

            match sqlx::query_as::<_, (String, DateTime<Utc>)>(
                r#"
            UPDATE _pg_lease
            SET held_until = NOW() + $3::interval
            WHERE name = $1 AND worker_id = $2 AND held_until > NOW()
            RETURNING worker_id, held_until"#,
            )
            .bind(lease_name.as_str())
            .bind(worker_id.as_str())
            .bind(options.lease_duration)
            .fetch_one(&pool)
            .await
            {
                Ok(result) => {
                    if result.0 != worker_id {
                        println!("lost lease during heartbeat: {}", worker_id);
                        abort_handle.abort();
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("Error during heartbeat for {}: {}", worker_id, e);
                    println!("lost lease during heartbeat due to error: {}", worker_id);
                    abort_handle.abort();
                    return;
                }
            }
        }
    }

    async fn drop_lease(
        pool: &Pool<Postgres>,
        lease_name: &str,
        worker_id: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        sqlx::query(
            r#"
            DELETE FROM _pg_lease WHERE name = $1 AND worker_id = $2"#,
        )
        .bind(lease_name)
        .bind(worker_id)
        .execute(pool)
        .await?;

        println!("successfully dropped lease: {}", worker_id);
        Ok(())
    }

    pub async fn verify_lease_held(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let result: (String, DateTime<Utc>) = sqlx::query_as(
            r#"
            SELECT worker_id, held_until FROM _pg_lease WHERE name = $1 AND worker_id = $2"#,
        )
        .bind(self.lease_name.as_str())
        .bind(self.worker_id.as_str())
        .fetch_one(&mut **tx)
        .await?;

        Ok(result.0 == self.worker_id)
    }

    pub async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut abort_handle = self.abort_handle.lock().await;
        if let Some(abort_handle) = abort_handle.take() {
            abort_handle.abort();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
