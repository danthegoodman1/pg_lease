package pg_lease

import "time"

type options struct {
	loopInterval       time.Duration
	loopIntervalJitter time.Duration
	leaseDuration      time.Duration
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

func WithLeaseDuration(duration time.Duration) OptionFunc {
	return func(o *options) {
		o.leaseDuration = duration
	}
}

func defaultOptions() *options {
	return &options{
		loopInterval:       time.Second,
		loopIntervalJitter: time.Duration(0),
		leaseDuration:      time.Minute * 5, // 5 minute default lease duration
	}
}
