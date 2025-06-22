package pg_lease

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	looper := NewLeaseLooper(func(ctx context.Context) error {
		t.Logf("Worker acquired lease %s", leaseName)
		leaseHeld <- true

		// Keep the lease for 2x the lease duration to test heartbeating
		select {
		case <-time.After(2 * leaseDuration):
			t.Logf("Worker held lease for 2x duration, returning")
			leaseComplete <- true
			return nil
		case <-ctx.Done():
			t.Logf("Worker context canceled: %v", ctx.Err())
			return ctx.Err()
		}
	}, "heartbeat-worker", leaseName, pool,
		WithLeaseDuration(leaseDuration),
		WithLoopInterval(loopInterval))

	looper.Start()
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
	looper1 := NewLeaseLooper(func(ctx context.Context) error {
		t.Logf("Worker-1 acquired lease %s", leaseName)
		worker1Got <- "worker-1"
		time.Sleep(1 * time.Second)
		t.Logf("Worker-1 returning to drop lease %s", leaseName)
		worker1Done <- struct{}{}
		return nil // Return to drop the lease
	}, "worker-1", leaseName, pool,
		WithLeaseDuration(leaseDuration),
		WithLoopInterval(loopInterval))

	// Worker 2: Waits to get the lease
	looper2 := NewLeaseLooper(func(ctx context.Context) error {
		t.Logf("Worker-2 acquired lease %s", leaseName)
		worker2Got <- "worker-2"
		<-ctx.Done() // Hold until stopped
		return ctx.Err()
	}, "worker-2", leaseName, pool,
		WithLeaseDuration(leaseDuration),
		WithLoopInterval(loopInterval))

	// Start worker 1 first
	looper1.Start()

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
	looper2.Start()
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
