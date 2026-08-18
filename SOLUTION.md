# Solution: Webhook Ingest

## What was broken (and why)
When I started digging into the repo, I found four main issues causing the bugs:
1. **TOCTOU race on inserts:** The `EventExists` check was happening before the insert. If two identical webhooks hit the server at the exact same time, both would pass the check and get inserted, leading to duplicate calls.
2. **Context cancellation:** The goroutine processing the recordings was just inheriting the HTTP request's context. So the moment the server returned `200 OK`, the framework canceled that context and killed the background download and database updates.
3. **No graceful shutdown:** When deployments happened, the server would shut down the HTTP listener but didn't wait for any background goroutines to finish. Any in-flight processing just disappeared into the void.
4. **Cache data race:** `stats.Cache.Record` was updating its internal map without a mutex. This is a classic Go data race and was likely dropping increments or panicking under load.

## Deduplication Strategy
To fix the idempotency issue, I decided to handle it straight at the database level by adding a `UNIQUE` constraint on `event_id` and using `ON CONFLICT DO NOTHING`. 

I went with this approach over using Redis because:
- The database gives us ACID guarantees essentially for free. We don't have to worry about the overhead or edge cases of distributed locks.
- Given the current scale, `ON CONFLICT` is super fast and combines the lock and insert into a single query.
- It completely removes the need for that race-prone `EventExists` check in our Go code, keeping the DB as the single source of truth.

## Scaling to 10k webhooks/second
If we suddenly had to process 10,000 webhooks a second, this architecture would probably fall over from Postgres connection exhaustion or too many goroutines. Here's how I'd change it:
1. **Move to a Message Queue:** The API handler shouldn't touch Postgres at all. It should just do basic validation and immediately push the event onto a queue like Kafka, RabbitMQ, or Redis Streams.
2. **Worker Pool:** I'd add a dedicated pool of workers to consume from that queue. This lets us carefully control concurrency and database connections without dropping things.
3. **Redis Deduplication:** At 10k/sec, relying on a Postgres unique constraint would cause too much index contention. We'd want to deduplicate using Redis (like a `SETNX` with a TTL) right at the edge before the events even reach the queue.
