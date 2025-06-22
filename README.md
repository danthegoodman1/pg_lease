# pg_lease

Go and Rust package for Postgres lease management package for running a single looping function while a lease is held.

With the current design, if a worker dies and then restarts, it can actually recover its lease before the timeout!

## Architecture

### Not using LISTEN/NOTIFY

While this could be use to optimize for recovery, I chose not to use it for 2 reasons:

1. Requires an extra connection
2. Leases can be lost due to timeouts (crashed lease holder, hangs, etc.), so need polling anyway
