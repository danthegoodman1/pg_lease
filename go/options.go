package pg_lease

import "time"

type options struct {
	loopInterval       time.Duration
	loopIntervalJitter time.Duration
}

type OptionFunc func(*options)

func WithLoopInterval(interval time.Duration) OptionFunc {
	return func(o *options) {
		o.loopInterval = interval
	}
}

func WithLoopIntervalJitter(intervalJitter time.Duration) OptionFunc {
	return func(o *options) {
		o.loopIntervalJitter = intervalJitter
	}
}

func defaultOptions() *options {
	return &options{
		loopInterval:       time.Second,
		loopIntervalJitter: time.Duration(0),
	}
}
