package pg_lease

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaseLooperFunc is called when the lease is held, and should loop while it wants to
// keep the lease.
//
// The ctx will be canceled when the lease is lost.
//
// Returning from this function drops the lease.
type LeaseLooperFunc func(ctx context.Context) error

type LeaseLooper struct {
	looperFunc LeaseLooperFunc
	options    Options
	leaseName  string
	pool       *pgxpool.Pool
	cancelFunc context.CancelFunc
	workerID   string
}

func NewLeaseLooper(looperFunc LeaseLooperFunc, workerID string, leaseName string, pool *pgxpool.Pool, options Options) *LeaseLooper {
	looper := &LeaseLooper{
		options:    options,
		looperFunc: looperFunc,
		leaseName:  leaseName,
		pool:       pool,
		workerID:   workerID,
	}

	if looper.options.LoopIntervalJitter == 0 {
		// we need something, if 0 it panics
		looper.options.LoopIntervalJitter = time.Nanosecond
	}

	return looper
}

func (looper *LeaseLooper) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	looper.cancelFunc = cancel

	return looper.launch(ctx)
}

func (looper *LeaseLooper) Stop() {
	looper.cancelFunc()
}

func (looper *LeaseLooper) launch(ctx context.Context) error {
	fmt.Println("launched lease looper, attempting to create table", looper.workerID)

	// create the table if not exists
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	conn, err := looper.pool.Acquire(timeoutCtx)
	if err != nil {
		return fmt.Errorf("error acquiring connection for table creation: %w", err)
	}
	_, err = conn.Exec(timeoutCtx, `create table if not exists _pg_lease (
    name text,
    worker_id text,
    held_until timestamptz,
    primary key (name)
)`)
	conn.Release()
	if err != nil {
		return fmt.Errorf("error creating _pg_lease table %s: %w - aborting", looper.workerID, err)
	}

	for {
		acquireChan := make(chan struct{}) // the lease ID

		go looper.acquireLease(ctx, acquireChan) // attempt to acquire the lease (also in a loop)

		select {
		case <-ctx.Done():
			fmt.Println("context done, exiting")
			return nil
		case <-acquireChan:
			fmt.Println("acquired lease, starting lease loop")
		}

		err = looper.leaseHandler(ctx)
		if err == nil {
			fmt.Println("LeaseLooperFunc returned, dropping lease")
			// Use a fresh context with timeout for dropping the lease
			dropCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			looper.dropLease(dropCtx)
			return nil
		} else if errors.Is(err, context.Canceled) {
			fmt.Println("context canceled, exiting")
			return nil
		} else {
			return fmt.Errorf("leaseHandler returned an error: %w", err)
		}
	}
}

// acquireLease will spin attempting to acquire a lease on the given options.loopInterval
func (looper *LeaseLooper) acquireLease(ctx context.Context, acquireChan chan struct{}) {
	fmt.Println("attempting to acquire lease", looper.workerID)
	for {
		// Try to acquire the lease
		acquired := func() bool {
			conn, err := looper.pool.Acquire(ctx)
			if err != nil {
				fmt.Println("[ERR]", fmt.Errorf("error acquiring connection %s: %w", looper.workerID, err))
				return false
			}
			defer conn.Release()

			// Try to insert the lease record if the record doesn't exist, or if it's expired
			var resultWorkerID string
			var resultHeldUntil time.Time
			err = conn.QueryRow(ctx, `
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
				RETURNING worker_id, held_until`,
				looper.leaseName, looper.workerID, looper.options.LeaseDuration).Scan(&resultWorkerID, &resultHeldUntil)

			if err != nil {
				fmt.Println("[ERR]", fmt.Errorf("error in lease query %s: %w", looper.workerID, err))
				return false
			}

			// Check if we successfully acquired the lease
			if resultWorkerID == looper.workerID {
				fmt.Println("successfully acquired lease", looper.workerID)
				return true
			} else {
				// Someone else holds the lease and it's not expired
				fmt.Println("lease held by another worker:", resultWorkerID, "until:", resultHeldUntil, looper.workerID)
				return false
			}
		}()

		if acquired {
			acquireChan <- struct{}{}
			return
		}

		// Sleep before the next attempt
		sleepDuration := looper.options.LoopInterval + time.Duration(rand.Int63n(int64(looper.options.LoopIntervalJitter)))
		fmt.Println("sleeping for", sleepDuration, looper.workerID)
		select {
		case <-ctx.Done():
			fmt.Println("context canceled in acquireLease, exiting", looper.workerID)
			return
		case <-time.After(sleepDuration):
			// Continue to the next iteration
		}
	}
}

// leaseHandler will loop while the lease is held, and will call the looperFunc.
// It returned when either:
// - the lease is lost
// - the looperFunc returns
// - the ctx is canceled
func (looper *LeaseLooper) leaseHandler(ctx context.Context) error {
	// context just for canceling the looperFunc if we lose the lease
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	looperFuncChan := make(chan error)
	go func() {
		looperFuncChan <- looper.looperFunc(leaseCtx)
	}()

	go looper.launchHeartbeatLoop(leaseCtx, cancel)

	select {
	case err := <-looperFuncChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// launchHeartbeatLoop runs in a background goroutine to renew the lease periodically
func (looper *LeaseLooper) launchHeartbeatLoop(ctx context.Context, cancel context.CancelFunc) {
	for {
		sleepDuration := looper.options.LoopInterval + time.Duration(rand.Int63n(int64(looper.options.LoopIntervalJitter)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDuration):
			if !looper.renewLease(ctx) {
				cancel() // Lost the lease, cancel the context
				return
			}
		}
	}
}

// renewLease attempts to renew the lease and returns true if successful
func (looper *LeaseLooper) renewLease(ctx context.Context) bool {
	conn, err := looper.pool.Acquire(ctx)
	if err != nil {
		fmt.Println("error acquiring connection for heartbeat:", err.Error(), looper.workerID)
		return false
	}
	defer conn.Release()

	var resultWorkerID string
	err = conn.QueryRow(ctx, `
		UPDATE _pg_lease
		SET held_until = NOW() + $3::interval
		WHERE name = $1 AND worker_id = $2 AND held_until > NOW()
		RETURNING worker_id`,
		looper.leaseName, looper.workerID, looper.options.LeaseDuration).Scan(&resultWorkerID)

	if err != nil || resultWorkerID != looper.workerID {
		fmt.Println("lost lease during heartbeat", looper.workerID)
		return false
	}

	return true
}

// dropLease attempts to delete the lease if we still own it
func (looper *LeaseLooper) dropLease(ctx context.Context) {
	conn, err := looper.pool.Acquire(ctx)
	if err != nil {
		fmt.Println("error acquiring connection for dropping lease:", err.Error(), looper.workerID)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, `
		DELETE FROM _pg_lease
		WHERE name = $1 AND worker_id = $2`,
		looper.leaseName, looper.workerID)

	if err != nil {
		fmt.Println("error dropping lease:", err.Error(), looper.workerID)
	} else {
		fmt.Println("successfully dropped lease", looper.workerID)
	}
}

// VerifyLeaseHeld will transactionally verify that a lease is still held
func (looper *LeaseLooper) VerifyLeaseHeld(ctx context.Context, txn pgx.Tx) (bool, error) {
	var resultWorkerID string
	err := txn.QueryRow(ctx, `
		SELECT worker_id
		FROM _pg_lease
		WHERE name = $1 AND worker_id = $2 AND held_until > NOW()`,
		looper.leaseName, looper.workerID).Scan(&resultWorkerID)

	if errors.Is(err, pgx.ErrNoRows) {
		// No rows found means we don't hold the lease
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return resultWorkerID == looper.workerID, nil
}
