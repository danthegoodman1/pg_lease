package pg_lease

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaseLooperFunc is called when the lease is held, and should loop while it wants to
// keep the lease.
//
// The ctx will be canceled when the lease is lost.
//
// Returning from this function drops the lease.
type LeaseLooperFunc func(ctx context.Context)

type LeaseLooper struct {
	looperFunc LeaseLooperFunc
	options    *options
	leaseName  string
	pool       pgxpool.Pool
	cancelFunc context.CancelFunc
	leaseID    string
	workerID   string
}

func NewLeaseLooper(looperFunc LeaseLooperFunc, workerID string, leaseName string, pool pgxpool.Pool, opts ...OptionFunc) *LeaseLooper {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	looper := &LeaseLooper{
		options:    options,
		looperFunc: looperFunc,
		leaseName:  leaseName,
		pool:       pool,
		workerID:   workerID,
	}

	return looper
}

func (looper *LeaseLooper) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	looper.cancelFunc = cancel

	go looper.launch(ctx)
}

func (looper *LeaseLooper) Stop() {
	looper.cancelFunc()
}

func (looper *LeaseLooper) launch(ctx context.Context) {
	fmt.Println("launched lease looper, attempting to acquire pool connection", looper.workerID)
	conn, err := looper.pool.Acquire(ctx)
	if err != nil {
		fmt.Println("[ERR] error in pool.Acquire:", err.Error())
		return
	}

	fmt.Println("acquired pool connection, attempting to acquire lease", looper.workerID)

	// create the table if not exists
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	_, err = conn.Exec(timeoutCtx, `create table if not exists _pg_lease (
    name text,
    worker_id text,
    held_until timestamptz,
    primary key (name)
)`, looper.leaseName)
	if err != nil {
		fmt.Println("[ERR]", fmt.Errorf("error creating _pg_lease table %s: %w - aborting", looper.workerID, err))
		return
	}

	for {
		looper.leaseID = ""              // clear lease id
		acquireChan := make(chan string) // the lease ID

		go looper.acquireLease(ctx, conn, acquireChan) // attempt to acquire the lease (also in a loop)

		select {
		case <-ctx.Done():
			fmt.Println("context done, exiting")
			return
		case looper.leaseID = <-acquireChan:
			fmt.Println("acquired lease, starting lease loop")
		}

		err = looper.leaseHandler(ctx)
		if err == nil {
			fmt.Println("LeaseLooperFunc returned, dropping lease")
		} else if errors.Is(err, context.Canceled) {
			fmt.Println("context canceled, exiting")
		} else {
			fmt.Println("[ERR] leaseHandler returned an error:", err.Error())
		}
	}
}

// acquireLease will spin attempting to acquire a lease on the given options.loopInterval
func (looper *LeaseLooper) acquireLease(ctx context.Context, conn *pgxpool.Conn, acquireChan chan string) {
	fmt.Println("attempting to acquire lease", looper.workerID)
	for {
		sleepDuration := looper.options.loopInterval + time.Duration(rand.Int63n(int64(looper.options.loopIntervalJitter)))
		fmt.Println("sleeping for", sleepDuration, looper.workerID)
		select {
		case <-ctx.Done():
			fmt.Println("context canceled in acquireLease, exiting", looper.workerID)
		case <-time.After(sleepDuration):
			// Try to acquire the lease
			tx, err := conn.Begin(ctx)
			if err != nil {
				fmt.Println("[ERR]", fmt.Errorf("error in conn.Begin %s: %w", looper.workerID, err))
				return
			}

			// Try to insert the lease record if the record doesn't exist, or if it's expired
			var resultWorkerID string
			var resultHeldUntil time.Time
			err = tx.QueryRow(ctx, `
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
				looper.leaseName, looper.workerID, looper.options.leaseDuration).Scan(&resultWorkerID, &resultHeldUntil)

			if err != nil {
				fmt.Println("[ERR]", fmt.Errorf("error in lease query %s: %w", looper.workerID, err))
				tx.Rollback(ctx)
				continue
			}

			// Check if we successfully acquired the lease
			if resultWorkerID == looper.workerID {
				err = tx.Commit(ctx)
				if err != nil {
					fmt.Println("[ERR]", fmt.Errorf("error committing lease transaction %s: %w", looper.workerID, err))
					continue
				}

				// Generate a unique lease ID for this acquisition
				leaseID := fmt.Sprintf("%s-%d", looper.workerID, rand.Int63())
				fmt.Println("successfully acquired lease", looper.workerID, "lease ID:", leaseID)
				acquireChan <- leaseID
				return
			} else {
				// Someone else holds the lease and it's not expired
				tx.Rollback(ctx)
				fmt.Println("lease held by another worker:", resultWorkerID, "until:", resultHeldUntil, looper.workerID)
			}
		}
	}
}

func (looper *LeaseLooper) leaseHandler(ctx context.Context) error {
	panic("todo")
}

// VerifyLeaseHeld will transactionally verify that a lease is still held
func VerifyLeaseHeld(ctx context.Context, txn pgxpool.Tx, looper *LeaseLooper) (bool, error) {
	panic("todo")
}
