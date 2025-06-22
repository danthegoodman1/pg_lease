use std::{error::Error, future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use rand::Rng;
use sqlx::{Pool, Postgres};
use tokio::{sync::Mutex, task::AbortHandle};

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

        let jitter_millis = if options.loop_interval_jitter.as_millis() > 0 {
            rand::rng().random_range(0..options.loop_interval_jitter.as_millis()) as u64
        } else {
            0
        };
        let sleep_duration = options.loop_interval + Duration::from_millis(jitter_millis);

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
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout};

    const TEST_DB_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";

    #[tokio::test]
    async fn test_lease_heartbeat() -> Result<(), Box<dyn Error + Send + Sync>> {
        let pool = sqlx::PgPool::connect(TEST_DB_URL).await?;

        let lease_name = format!(
            "test-lease-heartbeat-{}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let lease_duration = Duration::from_secs(2);
        let loop_interval = Duration::from_millis(500);

        let (lease_held_tx, mut lease_held_rx) = mpsc::channel::<bool>(1);
        let (lease_complete_tx, mut lease_complete_rx) = mpsc::channel::<bool>(1);

        let lease_held_tx = Arc::new(tokio::sync::Mutex::new(Some(lease_held_tx)));
        let lease_complete_tx = Arc::new(tokio::sync::Mutex::new(Some(lease_complete_tx)));

        let looper_func = {
            let lease_held_tx = lease_held_tx.clone();
            let lease_complete_tx = lease_complete_tx.clone();
            let lease_name = lease_name.clone();
            move || {
                let lease_held_tx = lease_held_tx.clone();
                let lease_complete_tx = lease_complete_tx.clone();
                let lease_name = lease_name.clone();
                async move {
                    println!("Worker acquired lease {}", lease_name);

                    if let Some(tx) = lease_held_tx.lock().await.take() {
                        let _ = tx.send(true).await;
                    }

                    // Keep the lease for 2x the lease duration to test heartbeating
                    sleep(2 * lease_duration).await;
                    println!("Worker held lease for 2x duration, returning");

                    if let Some(tx) = lease_complete_tx.lock().await.take() {
                        let _ = tx.send(true).await;
                    }

                    Ok(())
                }
            }
        };

        let options = LeaseLooperOptions {
            loop_interval,
            loop_interval_jitter: Duration::from_secs(0),
            lease_duration,
            lease_heartbeat_interval: Duration::from_millis(500), // Much shorter than lease duration
        };

        let looper = LeaseLooper::new(
            lease_name.clone(),
            looper_func,
            "heartbeat-worker".to_string(),
            pool.clone(),
            options,
        );

        // Start the looper
        looper.start().await?;

        // Wait for lease to be acquired
        let lease_acquired = timeout(Duration::from_secs(5), lease_held_rx.recv()).await;
        match lease_acquired {
            Ok(Some(true)) => println!("Successfully acquired lease {}", lease_name),
            _ => panic!("Failed to acquire lease {} within 5 seconds", lease_name),
        }

        // Wait for the worker to complete its 4-second hold
        let lease_completed = timeout(Duration::from_secs(6), lease_complete_rx.recv()).await;
        match lease_completed {
            Ok(Some(true)) => {
                println!("Successfully held lease for 2x duration via heartbeating");
                looper.stop().await?;
                Ok(())
            }
            _ => {
                looper.stop().await?;
                panic!("Worker did not complete 4-second hold - heartbeating may have failed");
            }
        }
    }

    #[tokio::test]
    async fn test_lease_drop_on_return() -> Result<(), Box<dyn Error + Send + Sync>> {
        let pool = sqlx::PgPool::connect(TEST_DB_URL).await?;

        let lease_name = format!(
            "test-lease-drop-{}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let lease_duration = Duration::from_secs(5);
        let loop_interval = Duration::from_millis(200);

        let (worker1_got_tx, mut worker1_got_rx) = mpsc::channel::<String>(1);
        let (worker1_done_tx, mut worker1_done_rx) = mpsc::channel::<bool>(1);
        let (worker2_got_tx, mut worker2_got_rx) = mpsc::channel::<String>(1);

        // Worker 1: Returns after 1 second
        let worker1_func = {
            let worker1_got_tx = worker1_got_tx.clone();
            let worker1_done_tx = worker1_done_tx.clone();
            let lease_name = lease_name.clone();
            move || {
                let worker1_got_tx = worker1_got_tx.clone();
                let worker1_done_tx = worker1_done_tx.clone();
                let lease_name = lease_name.clone();
                async move {
                    println!("Worker-1 acquired lease {}", lease_name);
                    let _ = worker1_got_tx.send("worker-1".to_string()).await;

                    sleep(Duration::from_secs(1)).await;
                    println!("Worker-1 returning to drop lease {}", lease_name);
                    let _ = worker1_done_tx.send(true).await;

                    Ok(()) // Return to drop the lease
                }
            }
        };

        // Worker 2: Waits to get the lease
        let worker2_func = {
            let worker2_got_tx = worker2_got_tx.clone();
            let lease_name = lease_name.clone();
            move || {
                let worker2_got_tx = worker2_got_tx.clone();
                let lease_name = lease_name.clone();
                async move {
                    println!("Worker-2 acquired lease {}", lease_name);
                    let _ = worker2_got_tx.send("worker-2".to_string()).await;

                    // Hold until we get cancelled (when test ends)
                    loop {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };

        let options = LeaseLooperOptions {
            loop_interval,
            loop_interval_jitter: Duration::from_secs(0),
            lease_duration,
            lease_heartbeat_interval: Duration::from_secs(1),
        };

        let looper1 = LeaseLooper::new(
            lease_name.clone(),
            worker1_func,
            "worker-1".to_string(),
            pool.clone(),
            options,
        );

        let looper2 = LeaseLooper::new(
            lease_name.clone(),
            worker2_func,
            "worker-2".to_string(),
            pool.clone(),
            options,
        );

        // Start worker 1 first
        looper1.start().await?;

        // Wait for worker 1 to get the lease
        let worker1_acquired = timeout(Duration::from_secs(5), worker1_got_rx.recv()).await;
        match worker1_acquired {
            Ok(Some(worker_id)) => {
                if worker_id != "worker-1" {
                    panic!("Expected worker-1, got {}", worker_id);
                }
                println!("Worker-1 successfully acquired lease {}", lease_name);
            }
            _ => panic!(
                "Worker-1 failed to get lease {} within 5 seconds",
                lease_name
            ),
        }

        sleep(Duration::from_millis(100)).await;

        // Now start worker 2
        println!("Starting worker 2");
        looper2.start().await?;

        // Wait for worker 1 to finish - it should drop the lease naturally
        let worker1_finished = timeout(Duration::from_secs(3), worker1_done_rx.recv()).await;
        match worker1_finished {
            Ok(Some(true)) => {
                println!("Worker-1 finished and should have dropped lease naturally");
                // Give a moment for the lease to be dropped
                sleep(Duration::from_millis(200)).await;
            }
            _ => panic!("Worker-1 did not finish within 3 seconds"),
        }

        // Wait for worker 2 to get the lease after worker 1 returns
        let worker2_acquired = timeout(Duration::from_secs(5), worker2_got_rx.recv()).await;
        match worker2_acquired {
            Ok(Some(worker_id)) => {
                if worker_id != "worker-2" {
                    panic!("Expected worker-2, got {}", worker_id);
                }
                println!(
                    "Worker-2 successfully acquired lease {} after worker-1 dropped it",
                    lease_name
                );
                // Clean up both loopers
                looper1.stop().await?;
                looper2.stop().await?;
                Ok(())
            }
            _ => {
                // Clean up both loopers on failure
                looper1.stop().await?;
                looper2.stop().await?;
                panic!(
                    "Worker-2 failed to get lease {} within 5 seconds after worker-1 returned",
                    lease_name
                );
            }
        }
    }

    #[tokio::test]
    async fn test_verify_lease_held() -> Result<(), Box<dyn Error + Send + Sync>> {
        let pool = sqlx::PgPool::connect(TEST_DB_URL).await?;

        let lease_name = format!(
            "test-lease-verify-{}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let lease_duration = Duration::from_secs(2);
        let loop_interval = Duration::from_millis(500);

        let (lease_held_tx, mut lease_held_rx) = mpsc::channel::<bool>(1);
        let (verify_result_tx, mut verify_result_rx) = mpsc::channel::<bool>(1);

        let looper_func = {
            let lease_held_tx = lease_held_tx.clone();
            let verify_result_tx = verify_result_tx.clone();
            let lease_name = lease_name.clone();
            let pool = pool.clone();
            move || {
                let lease_held_tx = lease_held_tx.clone();
                let verify_result_tx = verify_result_tx.clone();
                let lease_name = lease_name.clone();
                let pool = pool.clone();
                async move {
                    println!("Worker acquired lease {}", lease_name);
                    let _ = lease_held_tx.send(true).await;

                    // Start a transaction to verify the lease is held
                    let mut tx = pool.begin().await?;

                    // Create a temporary LeaseLooper instance to call verify_lease_held
                    let temp_looper = LeaseLooper::new(
                        lease_name.clone(),
                        || async { Ok(()) },
                        "verify-worker".to_string(),
                        pool.clone(),
                        LeaseLooperOptions::default(),
                    );

                    let held = temp_looper.verify_lease_held(&mut tx).await?;
                    tx.rollback().await?;

                    println!("VerifyLeaseHeld returned: {}", held);
                    let _ = verify_result_tx.send(held).await;

                    // Hold the lease for a bit then return
                    sleep(Duration::from_millis(500)).await;
                    Ok(())
                }
            }
        };

        let options = LeaseLooperOptions {
            loop_interval,
            loop_interval_jitter: Duration::from_secs(0),
            lease_duration,
            lease_heartbeat_interval: Duration::from_millis(500),
        };

        let looper = LeaseLooper::new(
            lease_name.clone(),
            looper_func,
            "verify-worker".to_string(),
            pool.clone(),
            options,
        );

        // Start the looper
        looper.start().await?;

        // Wait for lease to be acquired
        let lease_acquired = timeout(Duration::from_secs(5), lease_held_rx.recv()).await;
        match lease_acquired {
            Ok(Some(true)) => println!("Successfully acquired lease {}", lease_name),
            _ => panic!("Failed to acquire lease {} within 5 seconds", lease_name),
        }

        // Wait for verification result
        let verify_result = timeout(Duration::from_secs(2), verify_result_rx.recv()).await;
        match verify_result {
            Ok(Some(held)) => {
                if !held {
                    looper.stop().await?;
                    panic!("Expected VerifyLeaseHeld to return true when lease is held, got false");
                } else {
                    println!("VerifyLeaseHeld correctly returned true for held lease");
                    looper.stop().await?;
                    Ok(())
                }
            }
            _ => {
                looper.stop().await?;
                panic!("VerifyLeaseHeld did not return result within 2 seconds");
            }
        }
    }

    #[tokio::test]
    async fn test_lease_heartbeat_failure() -> Result<(), Box<dyn Error + Send + Sync>> {
        let pool = sqlx::PgPool::connect(TEST_DB_URL).await?;

        let lease_name = format!(
            "test-lease-heartbeat-fail-{}",
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let lease_duration = Duration::from_millis(800); // Short lease duration
        let loop_interval = Duration::from_millis(100);

        let (worker1_got_tx, mut worker1_got_rx) = mpsc::channel::<bool>(1);
        let (worker2_got_tx, mut worker2_got_rx) = mpsc::channel::<bool>(1);

        // Worker 1: Has broken heartbeat (too long interval)
        let worker1_func = {
            let worker1_got_tx = worker1_got_tx.clone();
            let lease_name = lease_name.clone();
            move || {
                let worker1_got_tx = worker1_got_tx.clone();
                let lease_name = lease_name.clone();
                async move {
                    println!("Worker-1 acquired lease {}", lease_name);
                    let _ = worker1_got_tx.send(true).await;

                    // This will run until cancelled due to lost lease from broken heartbeat
                    loop {
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        };

        // Worker 2: Has proper heartbeat to steal the lease
        let worker2_func = {
            let worker2_got_tx = worker2_got_tx.clone();
            let lease_name = lease_name.clone();
            move || {
                let worker2_got_tx = worker2_got_tx.clone();
                let lease_name = lease_name.clone();
                async move {
                    println!("Worker-2 acquired lease {}", lease_name);
                    let _ = worker2_got_tx.send(true).await;

                    // Hold until we get cancelled (when test ends)
                    loop {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };

        // Worker 1 options with broken heartbeat
        let worker1_options = LeaseLooperOptions {
            loop_interval,
            loop_interval_jitter: Duration::from_secs(0),
            lease_duration,
            lease_heartbeat_interval: Duration::from_secs(2), // Much longer than lease duration - should fail
        };

        // Worker 2 options with proper heartbeat
        let worker2_options = LeaseLooperOptions {
            loop_interval,
            loop_interval_jitter: Duration::from_secs(0),
            lease_duration,
            lease_heartbeat_interval: Duration::from_millis(200), // Proper heartbeat interval
        };

        let looper1 = LeaseLooper::new(
            lease_name.clone(),
            worker1_func,
            "worker-1".to_string(),
            pool.clone(),
            worker1_options,
        );

        let looper2 = LeaseLooper::new(
            lease_name.clone(),
            worker2_func,
            "worker-2".to_string(),
            pool.clone(),
            worker2_options,
        );

        // Start worker 1 first
        looper1.start().await?;

        // Wait for worker 1 to get the lease
        let worker1_acquired = timeout(Duration::from_secs(5), worker1_got_rx.recv()).await;
        match worker1_acquired {
            Ok(Some(true)) => println!("Worker-1 acquired lease"),
            _ => panic!("Worker-1 failed to acquire lease within 5 seconds"),
        }

        // Start worker 2 which should steal the lease
        looper2.start().await?;

        // Wait for worker 2 to steal the lease
        let worker2_acquired = timeout(Duration::from_secs(3), worker2_got_rx.recv()).await;
        match worker2_acquired {
            Ok(Some(true)) => println!("Worker-2 stole the lease"),
            _ => {
                looper1.stop().await?;
                looper2.stop().await?;
                panic!("Worker-2 should have stolen the lease within 3 seconds");
            }
        }

        // Give a moment for worker1 to detect it lost the lease and get cancelled
        sleep(Duration::from_millis(500)).await;

        // Stop both workers
        looper1.stop().await?;
        looper2.stop().await?;

        println!(
            "Test passed: Worker-2 successfully stole lease from Worker-1 with broken heartbeat"
        );
        Ok(())
    }
}
