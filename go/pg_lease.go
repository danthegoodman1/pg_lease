package pg_lease

import (
	"context"
	"errors"
	"fmt"

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
		select {}
	}
}

func (looper *LeaseLooper) leaseHandler(ctx context.Context) error {
	panic("todo")
}

// VerifyLeaseHeld will transactionally verify that a lease is still held
func VerifyLeaseHeld(ctx context.Context, txn pgxpool.Tx, looper *LeaseLooper) (bool, error) {
	panic("todo")
}
