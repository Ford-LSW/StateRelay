ALTER TABLE sr_worker
    ADD COLUMN reported_active_attempt_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[];

ALTER TABLE sr_dispatch
    ADD COLUMN transport_generation bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_sr_dispatch_transport_generation CHECK (transport_generation >= 0);
