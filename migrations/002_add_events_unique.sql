-- We need to guarantee idempotency by making event_id unique, so we can use ON CONFLICT.
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
