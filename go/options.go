package pg_lease

import "time"

type options struct {
	loopInterval time.Duration
}

type OptionFunc func(*options)

func WithLoopInterval(interval time.Duration) OptionFunc {
	return func(o *options) {
		o.loopInterval = interval
	}
}

func defaultOptions() *options {
	return &options{
		loopInterval: time.Second,
	}
}
