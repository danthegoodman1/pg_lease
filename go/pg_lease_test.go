package pg_lease

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDBURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

func TestLeaseHeartbeat(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	leaseName := fmt.Sprintf("test-lease-heartbeat-%d", time.Now().UnixNano())
	leaseDuration := 2 * time.Second
	loopInterval := 500 * time.Millisecond

	leaseHeld := make(chan bool, 1)
	leaseComplete := make(chan bool, 1)

	looper := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker acquired lease %s", leaseName)
		leaseHeld <- true

		// Keep the lease for 2x the lease duration to test heartbeating
		select {
		case <-time.After(2 * leaseDuration):
			t.Logf("Worker held lease for 2x duration, returning")
			leaseComplete <- true
			return nil
		case <-leaseContext.Context.Done():
			t.Logf("Worker context canceled: %v", leaseContext.Context.Err())
			return leaseContext.Context.Err()
		}
	}, "heartbeat-worker", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Millisecond * 500,
		})

	go func() {
		err := looper.Start()
		if err != nil {
			t.Errorf("Worker failed to start: %v", err)
		}
	}()
	defer looper.Stop()

	// Wait for lease to be acquired
	select {
	case <-leaseHeld:
		t.Logf("Successfully acquired lease %s", leaseName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Failed to acquire lease %s within 5 seconds", leaseName)
	}

	// Wait for the worker to complete its 4-second hold
	select {
	case <-leaseComplete:
		t.Logf("Successfully held lease for 2x duration via heartbeating")
	case <-time.After(6 * time.Second):
		t.Fatalf("Worker did not complete 4-second hold - heartbeating may have failed")
	}
}

func TestLeaseDropOnReturn(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	leaseName := fmt.Sprintf("test-lease-drop-%d", time.Now().UnixNano())
	leaseDuration := 5 * time.Second
	loopInterval := 200 * time.Millisecond

	worker1Got := make(chan string, 1)
	worker1Done := make(chan struct{}, 1)
	worker2Got := make(chan string, 1)

	// Worker 1: Returns after 1 second
	looper1 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-1 acquired lease %s", leaseName)
		worker1Got <- "worker-1"
		time.Sleep(1 * time.Second)
		t.Logf("Worker-1 returning to drop lease %s", leaseName)
		worker1Done <- struct{}{}
		return nil // Return to drop the lease
	}, "worker-1", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Second * 1,
		})

	// Worker 2: Waits to get the lease
	looper2 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-2 acquired lease %s", leaseName)
		worker2Got <- "worker-2"
		<-leaseContext.Context.Done() // Hold until stopped
		return leaseContext.Context.Err()
	}, "worker-2", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Second * 1,
		})

	// Start worker 1 first
	go func() {
		err := looper1.Start()
		if err != nil {
			t.Errorf("Worker-1 failed to start: %v", err)
		}
	}()

	// Wait for worker 1 to get the lease
	select {
	case workerID := <-worker1Got:
		if workerID != "worker-1" {
			t.Fatalf("Expected worker-1, got %s", workerID)
		}
		t.Logf("Worker-1 successfully acquired lease %s", leaseName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Worker-1 failed to get lease %s within 5 seconds", leaseName)
	}

	time.Sleep(100 * time.Millisecond)

	// Now start worker 2
	t.Logf("Starting worker 2")
	go func() {
		err := looper2.Start()
		if err != nil {
			t.Errorf("Worker-2 failed to start: %v", err)
		}
	}()
	defer looper2.Stop()

	// Wait for worker 1 to finish and then stop it
	select {
	case <-worker1Done:
		t.Logf("Worker-1 finished, stopping it")
		looper1.Stop()
	case <-time.After(3 * time.Second):
		t.Fatalf("Worker-1 did not finish within 3 seconds")
	}

	// Wait for worker 2 to get the lease after worker 1 returns
	select {
	case workerID := <-worker2Got:
		if workerID != "worker-2" {
			t.Fatalf("Expected worker-2, got %s", workerID)
		}
		t.Logf("Worker-2 successfully acquired lease %s after worker-1 dropped it", leaseName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Worker-2 failed to get lease %s within 5 seconds after worker-1 returned", leaseName)
	}
}

func TestVerifyLeaseHeld(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	leaseName := fmt.Sprintf("test-lease-verify-%d", time.Now().UnixNano())
	leaseDuration := 2 * time.Second
	loopInterval := 500 * time.Millisecond

	leaseHeld := make(chan bool, 1)
	verifyResult := make(chan bool, 1)

	var looper *LeaseLooper
	looper = NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker acquired lease %s", leaseName)
		leaseHeld <- true

		// Acquire a connection and start a transaction to verify the lease is held
		conn, err := pool.Acquire(leaseContext.Context)
		if err != nil {
			t.Errorf("Failed to acquire connection: %v", err)
			return err
		}
		defer conn.Release()

		txn, err := conn.BeginTx(leaseContext.Context, pgx.TxOptions{})
		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return err
		}

		held, err := looper.VerifyLeaseHeld(leaseContext.Context, txn)
		if err != nil {
			t.Errorf("VerifyLeaseHeld failed: %v", err)
			return fmt.Errorf("VerifyLeaseHeld failed: %w", err)
		}

		verifyResult <- held
		t.Logf("VerifyLeaseHeld returned: %v", held)

		// Hold the lease for a bit then return
		time.Sleep(500 * time.Millisecond)
		return nil
	}, "verify-worker", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Millisecond * 500,
		})

	go func() {
		err := looper.Start()
		if err != nil {
			t.Errorf("Worker failed to start: %v", err)
		}
	}()
	defer looper.Stop()

	// Wait for lease to be acquired
	select {
	case <-leaseHeld:
		t.Logf("Successfully acquired lease %s", leaseName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Failed to acquire lease %s within 5 seconds", leaseName)
	}

	// Wait for verification result
	select {
	case held := <-verifyResult:
		if !held {
			t.Errorf("Expected VerifyLeaseHeld to return true when lease is held, got false")
		} else {
			t.Logf("VerifyLeaseHeld correctly returned true for held lease")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("VerifyLeaseHeld did not return result within 2 seconds")
	}
}

func TestLeaseHeartbeatFailure(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	leaseName := fmt.Sprintf("test-lease-heartbeat-fail-%d", time.Now().UnixNano())
	leaseDuration := 800 * time.Millisecond // Short lease duration
	loopInterval := 100 * time.Millisecond

	worker1Got := make(chan bool, 1)
	worker1Lost := make(chan bool, 1)
	worker2Got := make(chan bool, 1)

	// Worker 1: Has broken heartbeat (too long interval)
	looper1 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-1 acquired lease %s", leaseName)
		worker1Got <- true

		// Wait for context cancellation due to lost lease
		<-leaseContext.Context.Done()
		t.Logf("Worker-1 lost lease as expected due to failed heartbeating")
		worker1Lost <- true
		return leaseContext.Context.Err()
	}, "worker-1", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Second * 2, // Much longer than lease duration - should fail
		})

	// Worker 2: Has proper heartbeat to steal the lease
	looper2 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-2 acquired lease %s", leaseName)
		worker2Got <- true
		<-leaseContext.Context.Done() // Hold until stopped
		return leaseContext.Context.Err()
	}, "worker-2", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Millisecond * 200, // Proper heartbeat interval
		})

	// Start worker 1 first
	go func() {
		err := looper1.Start()
		if err != nil {
			t.Logf("Worker-1 stopped: %v", err)
		}
	}()

	// Wait for worker 1 to get the lease
	select {
	case <-worker1Got:
		t.Logf("Worker-1 acquired lease")
	case <-time.After(5 * time.Second):
		t.Fatalf("Worker-1 failed to acquire lease within 5 seconds")
	}

	// Start worker 2 which should steal the lease
	go func() {
		err := looper2.Start()
		if err != nil {
			t.Logf("Worker-2 stopped: %v", err)
		}
	}()
	defer looper2.Stop()

	// Wait for worker 2 to steal the lease
	select {
	case <-worker2Got:
		t.Logf("Worker-2 stole the lease")
	case <-time.After(3 * time.Second):
		t.Fatalf("Worker-2 should have stolen the lease within 3 seconds")
	}

	// Wait for worker 1 to lose the lease
	select {
	case <-worker1Lost:
		t.Logf("Test passed: Worker-1 lost lease due to failed heartbeating")
	case <-time.After(2 * time.Second):
		t.Fatalf("Worker-1 should have lost lease within 2 seconds after worker-2 acquired it")
	}
}

func TestLeaseNoHeartbeat(t *testing.T) {
	// This test verifies that when LeaseHeartbeatInterval = 0:
	// 1. No heartbeat loop is launched (check logs for "launching heartbeat loop")
	// 2. Worker continues running even after lease expires in database
	// 3. Other workers can "steal" the expired lease while original worker still runs
	// 4. Original worker doesn't detect lease loss automatically (no context cancellation)

	pool, err := pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	leaseName := fmt.Sprintf("test-lease-no-heartbeat-%d", time.Now().UnixNano())
	leaseDuration := 1 * time.Second // Short lease duration
	loopInterval := 100 * time.Millisecond

	worker1Got := make(chan bool, 1)
	worker1Lost := make(chan bool, 1)
	worker2Got := make(chan bool, 1)

	// Worker 1: Has no heartbeating (LeaseHeartbeatInterval = 0)
	looper1 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-1 acquired lease %s", leaseName)
		worker1Got <- true

		// Keep running indefinitely - with no heartbeating, worker doesn't know when lease expires
		// Worker-2 should be able to steal the lease after it expires in the database
		<-leaseContext.Context.Done() // Wait until context is canceled (when we stop the looper)
		t.Logf("Worker-1 context canceled")
		worker1Lost <- true
		return leaseContext.Context.Err()
	}, "worker-1", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: 0, // DISABLE heartbeating - no background renewal
		})

	// Worker 2: Normal worker to acquire lease after worker 1 loses it
	looper2 := NewLeaseLooper(func(leaseContext LeaseContext) error {
		t.Logf("Worker-2 acquired lease %s", leaseName)
		worker2Got <- true
		<-leaseContext.Context.Done() // Hold until stopped
		return leaseContext.Context.Err()
	}, "worker-2", leaseName, pool,
		Options{
			LeaseDuration:          leaseDuration,
			LoopInterval:           loopInterval,
			LoopIntervalJitter:     time.Duration(0),
			LeaseHeartbeatInterval: time.Millisecond * 200, // Normal heartbeat
		})

	// Start worker 1 first
	go func() {
		err := looper1.Start()
		if err != nil {
			t.Logf("Worker-1 stopped: %v", err)
		}
	}()

	// Wait for worker 1 to get the lease
	select {
	case <-worker1Got:
		t.Logf("Worker-1 acquired lease")
	case <-time.After(3 * time.Second):
		t.Fatalf("Worker-1 failed to acquire lease within 3 seconds")
	}

	// Start worker 2 after a short delay to let worker 1 get established
	time.Sleep(200 * time.Millisecond)
	go func() {
		err := looper2.Start()
		if err != nil {
			t.Logf("Worker-2 stopped: %v", err)
		}
	}()
	defer looper2.Stop()

	// Wait for worker 2 to steal the lease (should happen after lease expires ~1 second)
	select {
	case <-worker2Got:
		t.Logf("Worker-2 stole the lease after it expired (proving no heartbeating on worker-1)")
		// Now stop worker-1 since worker-2 has the lease
		looper1.Stop()
	case <-time.After(3 * time.Second):
		t.Fatalf("Worker-2 should have stolen the lease within 3 seconds after lease expiration")
	}

	// Wait for worker 1 to finish after being stopped
	select {
	case <-worker1Lost:
		t.Logf("Worker-1 finished after being stopped")
	case <-time.After(2 * time.Second):
		t.Fatalf("Worker-1 should have finished within 2 seconds after being stopped")
	}
}
