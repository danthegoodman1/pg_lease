# pg_lease

Go and Rust package for Postgres lease management package for running a single looping function while a lease is held.

This is a great building block for distributed systems that need a simple mechanism to coordinate ownership of partitions via a single process making decisions. This can then be integrated with `VerifyLeaseHeld` to transactionally verify that the lease is still held when making updates.

Downstream dependencies (e.g. partitions) would asynchornously receive updates and use transactional methods to verify they own the partition when doing some operation (e.g. writing log segment metadata to postgres as the metadata store).

## Architecture

With the current design, if a worker dies and then restarts, it can actually recover its lease before the timeout!

### Not using LISTEN/NOTIFY

While this could be use to optimize for recovery, I chose not to use it for 2 reasons:

1. Requires an extra connection
2. Leases can be lost due to timeouts (crashed lease holder, hangs, etc.), so need polling anyway
