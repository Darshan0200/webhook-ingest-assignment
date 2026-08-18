# Solution: webhook-ingest

## What was broken and why
Four main defects were causing the misbehavior:
1. **Duplicate Webhooks (TOCTOU Race):** `EventExists` checked for duplicates before insertion. In concurrent scenarios, two identical deliveries would both pass the check and be inserted, causing duplicate calls and inflated account stats.
2. **Missing Recordings (Context Cancellation):** The background goroutine handling recording downloads inherited the HTTP request `context.Context`. When the handler returned 200 OK, the framework immediately canceled the context, killing the background database queries and sleep timers. Errors were also completely swallowed.
3. **Disappearing In-flight Work (No Graceful Background Shutdown):** During deployments, the server cleanly shut down the HTTP listener but had no mechanism to wait for running background goroutines, causing active tasks to drop mid-flight.
4. **Data Race in Cache:** `stats.Cache.Record` modified the internal map without acquiring a mutex lock, which in Go is a critical data race that can lead to panics or missing increments.

## Deduplication Strategy
I chose to handle deduplication at the database level by adding a `UNIQUE` constraint on `event_id` in Postgres and using an `INSERT ... ON CONFLICT DO NOTHING` statement. 
**Why this approach?**
- **Atomicity and Consistency:** Pushing the idempotency guarantee to a relational database provides robust atomicity without the overhead of maintaining distributed locks in Redis.
- **Performance:** For our current throughput, avoiding a roundtrip to Redis for locking logic reduces latency. `ON CONFLICT` effectively provides a free "lock" and insert in one operation.
- **Simplicity:** It eliminates the complex TOCTOU logic entirely and keeps our state in a single source of truth.

## Scaling to 10,000 webhooks/second
If throughput drastically increases to 10k/sec, the current architecture will face bottlenecks around Postgres connections, database locks, and unbounded goroutine spawning. Here's what I would change:
1. **Event Driven / Queueing:** Instead of doing database inserts and spawning ad-hoc goroutines per request, the API handler should merely validate the webhook and push it to a high-throughput message queue (like Kafka, AWS SQS, or Redis Streams).
2. **Worker Pool:** We would consume the queue with a dedicated worker pool. This controls concurrency, limits Postgres connections, and ensures tasks survive application restarts.
3. **Redis Deduplication:** At 10k/sec, relying on Postgres unique constraints for deduplication could cause heavy index contention. We would use Redis `SETNX` (or a distributed lock) with a TTL to deduplicate events extremely quickly before they ever touch the message queue.
