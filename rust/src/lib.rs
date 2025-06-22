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

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
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
    ) {
        println!("launching looper, attempting to create table {}", worker_id);

        sqlx::query(
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
        .unwrap();

        println!("table created, attempting to acquire lease: {}", worker_id);

        loop {
            Self::wait_for_lease(&pool, &lease_name, worker_id.as_str(), &options).await;

            // We have the lease

            let looper_handle = tokio::task::spawn(looper_func());

            // spawn heartbeat loop that can cancel future
            tokio::task::spawn(Self::heartbeat_loop(
                looper_handle.abort_handle(),
                pool.clone(),
                lease_name.clone(),
                worker_id.clone(),
                options,
            ));

            let looper_func_result = looper_handle.await.unwrap();

            if looper_func_result.is_err() {
                println!(
                    "[ERR]: looper func returned an error: {:?}",
                    looper_func_result
                );
            } else {
                println!("looper_func returned, dropping lease");
                Self::drop_lease(&pool, &lease_name, &worker_id).await;
            }
        }
    }

    async fn wait_for_lease(
        pool: &Pool<Postgres>,
        lease_name: &str,
        worker_id: &str,
        options: &LeaseLooperOptions,
    ) {
        println!("attempting to acquire lease: {}", worker_id);

        let sleep_duration = options.loop_interval
            + Duration::from_millis(
                rand::rng().random_range(0..options.loop_interval_jitter.as_millis()) as u64,
            );

        loop {
            let acquired = Self::try_acquire_lease(pool, lease_name, worker_id, options)
                .await
                .unwrap();
            if acquired {
                break;
            }

            tokio::time::sleep(sleep_duration).await;
        }

        println!("sleeping for: {:?}", sleep_duration);
    }

    async fn try_acquire_lease(
        pool: &Pool<Postgres>,
        lease_name: &str,
        worker_id: &str,
        options: &LeaseLooperOptions,
    ) -> Result<bool, Box<dyn Error>> {
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
        .await
        .unwrap();

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

            // try to heartbeat the lease
            let result: (String, DateTime<Utc>) = sqlx::query_as(
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
            .unwrap();

            if result.0 != worker_id {
                println!("lost lease during heartbeat: {}", worker_id);
                abort_handle.abort();
                return;
            }
        }
    }

    async fn drop_lease(pool: &Pool<Postgres>, lease_name: &str, worker_id: &str) {
        sqlx::query(
            r#"
            DELETE FROM _pg_lease WHERE name = $1 AND worker_id = $2"#,
        )
        .bind(lease_name)
        .bind(worker_id)
        .execute(pool)
        .await
        .unwrap();
    }

    pub async fn stop(&self) -> Result<(), Box<dyn Error>> {
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
