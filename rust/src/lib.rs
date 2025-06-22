use std::{error::Error, sync::Arc, time::Duration};

use sqlx::{Pool, Postgres};
use tokio::{sync::Mutex, task::AbortHandle};

pub struct LeaseLooper<T: AsyncFn()> {
    lease_name: String,
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

impl<T: AsyncFn()> LeaseLooper<T> {
    pub fn new(
        lease_name: String,
        looper_func: T,
        pool: Pool<Postgres>,
        options: LeaseLooperOptions,
    ) -> Self {
        Self {
            lease_name,
            looper_func,
            options,
            pool,
            abort_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        todo!()
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
