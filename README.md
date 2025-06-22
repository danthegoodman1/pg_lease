# pg_lease

Go and Rust package for Postgres lease management package for running a single looping function while a lease is held.

With the current design, if a worker dies and then restarts, it can actually recover its lease before the timeout!
