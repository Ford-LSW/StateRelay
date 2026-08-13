ALTER TABLE sr_worker
    ADD COLUMN queue_capacity integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_sr_worker_queue_capacity CHECK (queue_capacity >= 0);
