package pg_lease

import "time"

type Options struct {
	LoopInterval           time.Duration
	LoopIntervalJitter     time.Duration
	LeaseDuration          time.Duration
	LeaseHeartbeatInterval time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		LoopInterval:           time.Second,
		LoopIntervalJitter:     time.Duration(0),
		LeaseDuration:          time.Second * 10,
		LeaseHeartbeatInterval: time.Second * 3,
	}
}
