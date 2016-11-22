# ThreadPool

Manage a pool of threads used to run tasks. Tasks are added
to a wait queue, scheduled to run when a worker is available,
and the task return value is added to the done queue. Done tasks
may be reaped from the done queue. The scheduling of threads to
run may be enabled and disabled. At no time are running threads
interrupted or terminated. A threadpool may be drained at any
time to remove waiting tasks and halt further scheduling;
running threads are waited on until they complete.
