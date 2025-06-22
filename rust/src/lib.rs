use std::{error::Error, future::Future, sync::Arc, time::Duration};

use rand::Rng;
use sqlx::{Acquire, Pool, Postgres};
use tokio::{sync::Mutex, task::AbortHandle};

pub struct LeaseLooper<T, Fut>
where
    T: Fn() -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
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
    Fut: Future<Output = ()> + Send + 'static,
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
            self.worker_id.clone(),
        ));

        *abort_handle = Some(join_handle.abort_handle());

        Ok(())
    }

    async fn launch_looper(
        looper_func: T,
        pool: Pool<Postgres>,
        options: LeaseLooperOptions,
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
            Self::acquire_lease(&pool, worker_id.as_str(), &options).await;

            tokio::time::sleep(options.loop_interval).await;
            looper_func().await;
        }
    }

    async fn acquire_lease(pool: &Pool<Postgres>, worker_id: &str, options: &LeaseLooperOptions) {
        println!("attempting to acquire lease: {}", worker_id);

        let sleep_duration = options.loop_interval
            + Duration::from_millis(
                rand::rng().random_range(0..options.loop_interval_jitter.as_millis()) as u64,
            );

        println!("sleeping for: {:?}", sleep_duration);

        let mut conn = pool.acquire().await.unwrap();
        let tx = conn.begin().await.unwrap();
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
