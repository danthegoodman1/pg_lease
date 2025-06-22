package pg_lease

import "context"

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
}

func NewLeaseLooper(looperFunc LeaseLooperFunc, leaseName string, opts ...OptionFunc) *LeaseLooper {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	looper := &LeaseLooper{
		options:    options,
		looperFunc: looperFunc,
		leaseName:  leaseName,
	}

	return looper
}

func (looper *LeaseLooper) Start() {

}

func (looper *LeaseLooper) Stop() {

}
