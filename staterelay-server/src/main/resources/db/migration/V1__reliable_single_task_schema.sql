CREATE TABLE sr_application (
    id uuid PRIMARY KEY,
    name varchar(200) NOT NULL,
    environment varchar(100) NOT NULL DEFAULT 'default',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uk_sr_application_name_environment UNIQUE (name, environment)
);

CREATE TABLE sr_worker (
    id uuid PRIMARY KEY,
    application_id uuid NOT NULL REFERENCES sr_application(id),
    worker_key varchar(300) NOT NULL,
    worker_epoch uuid NOT NULL,
    pod_name varchar(253) NOT NULL,
    pod_ip inet NOT NULL,
    executor_port integer NOT NULL,
    status varchar(20) NOT NULL,
    max_concurrency integer NOT NULL,
    reserved_capacity integer NOT NULL DEFAULT 0,
    reported_active_count integer NOT NULL DEFAULT 0,
    queue_depth integer NOT NULL DEFAULT 0,
    handlers jsonb NOT NULL DEFAULT '[]'::jsonb,
    labels jsonb NOT NULL DEFAULT '{}'::jsonb,
    starter_version varchar(100),
    lease_expires_at timestamptz NOT NULL,
    last_heartbeat_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uk_sr_worker_key UNIQUE (application_id, worker_key),
    CONSTRAINT ck_sr_worker_status CHECK (status IN ('STARTING', 'READY', 'DRAINING', 'OFFLINE')),
    CONSTRAINT ck_sr_worker_port CHECK (executor_port BETWEEN 1 AND 65535),
    CONSTRAINT ck_sr_worker_capacity CHECK (
        max_concurrency > 0
        AND reserved_capacity >= 0
        AND reserved_capacity <= max_concurrency
        AND reported_active_count >= 0
        AND queue_depth >= 0
    )
);

CREATE INDEX ix_sr_worker_routing
    ON sr_worker(application_id, status, lease_expires_at, id);

CREATE TABLE sr_task_definition (
    id uuid PRIMARY KEY,
    application_id uuid NOT NULL REFERENCES sr_application(id),
    name varchar(200) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'DRAFT',
    current_published_version_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uk_sr_task_definition_name UNIQUE (application_id, name),
    CONSTRAINT ck_sr_task_definition_status CHECK (status IN ('DRAFT', 'PUBLISHED', 'DISABLED'))
);

CREATE TABLE sr_task_definition_version (
    id uuid PRIMARY KEY,
    task_definition_id uuid NOT NULL REFERENCES sr_task_definition(id),
    version_no integer NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'DRAFT',
    handler_name varchar(300),
    configuration_snapshot jsonb NOT NULL,
    parameter_schema jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    CONSTRAINT uk_sr_task_definition_version UNIQUE (task_definition_id, version_no),
    CONSTRAINT ck_sr_definition_version_no CHECK (version_no > 0),
    CONSTRAINT ck_sr_definition_version_status CHECK (status IN ('DRAFT', 'PUBLISHED', 'RETIRED'))
);

ALTER TABLE sr_task_definition
    ADD CONSTRAINT fk_sr_definition_published_version
    FOREIGN KEY (current_published_version_id) REFERENCES sr_task_definition_version(id);

CREATE TABLE sr_trigger (
    id uuid PRIMARY KEY,
    task_definition_id uuid NOT NULL REFERENCES sr_task_definition(id),
    definition_version_id uuid REFERENCES sr_task_definition_version(id),
    trigger_type varchar(30) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'ACTIVE',
    schedule_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    time_zone varchar(100),
    misfire_policy varchar(20) NOT NULL DEFAULT 'FIRE_ONCE',
    next_fire_at timestamptz,
    last_scheduled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_sr_trigger_type CHECK (trigger_type IN ('CRON', 'FIXED_RATE', 'FIXED_DELAY', 'ONE_TIME')),
    CONSTRAINT ck_sr_trigger_status CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED')),
    CONSTRAINT ck_sr_trigger_misfire CHECK (misfire_policy IN ('FIRE_ONCE', 'SKIP'))
);

CREATE INDEX ix_sr_trigger_due
    ON sr_trigger(next_fire_at, id)
    WHERE status = 'ACTIVE' AND next_fire_at IS NOT NULL;

CREATE TABLE sr_task_instance (
    id uuid PRIMARY KEY,
    task_definition_id uuid NOT NULL REFERENCES sr_task_definition(id),
    definition_version_id uuid NOT NULL REFERENCES sr_task_definition_version(id),
    trigger_id uuid REFERENCES sr_trigger(id),
    business_idempotency_key varchar(500),
    status varchar(30) NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    scheduled_at timestamptz NOT NULL,
    next_run_at timestamptz NOT NULL,
    configuration_snapshot jsonb NOT NULL,
    payload jsonb NOT NULL,
    current_lease_version bigint NOT NULL DEFAULT 0,
    claim_token uuid,
    claimed_at timestamptz,
    cancellation_requested_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_sr_instance_status CHECK (status IN (
        'WAITING', 'READY', 'RUNNING', 'RETRY_WAIT', 'CANCELLING',
        'SUCCESS', 'FAILED', 'CANCELLED')),
    CONSTRAINT ck_sr_instance_payload_size CHECK (
        octet_length(convert_to(payload::text, 'UTF8')) <= 65536),
    CONSTRAINT ck_sr_instance_lease_version CHECK (current_lease_version >= 0),
    CONSTRAINT ck_sr_instance_claim CHECK (
        (claim_token IS NULL AND claimed_at IS NULL)
        OR (claim_token IS NOT NULL AND claimed_at IS NOT NULL))
);

CREATE UNIQUE INDEX uk_sr_instance_trigger_time
    ON sr_task_instance(trigger_id, scheduled_at)
    WHERE trigger_id IS NOT NULL;

CREATE UNIQUE INDEX uk_sr_instance_business_idempotency
    ON sr_task_instance(task_definition_id, business_idempotency_key)
    WHERE business_idempotency_key IS NOT NULL;

CREATE INDEX ix_sr_instance_ready
    ON sr_task_instance(priority DESC, next_run_at, id)
    WHERE status IN ('READY', 'RETRY_WAIT');

CREATE TABLE sr_task_attempt (
    id uuid PRIMARY KEY,
    task_instance_id uuid NOT NULL REFERENCES sr_task_instance(id),
    attempt_no integer NOT NULL,
    lease_version bigint NOT NULL,
    worker_id uuid REFERENCES sr_worker(id),
    worker_epoch uuid,
    status varchar(30) NOT NULL,
    lease_expires_at timestamptz,
    progress_percent integer NOT NULL DEFAULT 0,
    progress_message varchar(1000),
    result jsonb,
    error_code varchar(200),
    error_message text,
    report_checksum varchar(128),
    capacity_released_at timestamptz,
    assigned_at timestamptz,
    accepted_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_sr_attempt_status CHECK (status IN (
        'CREATED', 'ASSIGNED', 'ACCEPTED', 'RUNNING', 'SUCCESS', 'FAILED',
        'CANCELLED', 'LOST', 'TIMED_OUT')),
    CONSTRAINT ck_sr_attempt_number CHECK (attempt_no > 0),
    CONSTRAINT ck_sr_attempt_progress CHECK (progress_percent BETWEEN 0 AND 100),
    CONSTRAINT ck_sr_attempt_lease_version CHECK (lease_version >= 0),
    CONSTRAINT ck_sr_attempt_result_size CHECK (
        result IS NULL OR octet_length(convert_to(result::text, 'UTF8')) <= 65536),
    CONSTRAINT ck_sr_attempt_worker_fence CHECK (
        (worker_id IS NULL AND worker_epoch IS NULL)
        OR (worker_id IS NOT NULL AND worker_epoch IS NOT NULL))
);

CREATE UNIQUE INDEX uk_sr_attempt_number
    ON sr_task_attempt(task_instance_id, attempt_no);

CREATE INDEX ix_sr_attempt_expired_lease
    ON sr_task_attempt(lease_expires_at, id)
    WHERE status IN ('ASSIGNED', 'ACCEPTED', 'RUNNING');

CREATE TABLE sr_dispatch (
    id uuid PRIMARY KEY,
    dispatch_id uuid NOT NULL,
    task_attempt_id uuid NOT NULL REFERENCES sr_task_attempt(id),
    target_worker_id uuid NOT NULL REFERENCES sr_worker(id),
    target_worker_epoch uuid NOT NULL,
    target_address varchar(1000) NOT NULL,
    status varchar(30) NOT NULL,
    transport_attempts integer NOT NULL DEFAULT 0,
    next_transport_at timestamptz,
    sent_at timestamptz,
    acknowledged_at timestamptz,
    expires_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_sr_dispatch_status CHECK (status IN ('PENDING', 'SENT', 'UNCERTAIN', 'ACKED', 'EXPIRED')),
    CONSTRAINT ck_sr_dispatch_attempts CHECK (transport_attempts >= 0)
);

CREATE UNIQUE INDEX uk_sr_dispatch_id ON sr_dispatch(dispatch_id);
CREATE UNIQUE INDEX uk_sr_attempt_dispatch ON sr_dispatch(task_attempt_id);

CREATE INDEX ix_sr_dispatch_pending
    ON sr_dispatch(next_transport_at, id)
    WHERE status IN ('PENDING', 'UNCERTAIN');

CREATE TABLE sr_outbox_event (
    id uuid PRIMARY KEY,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type varchar(200) NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    publication_lease_token uuid,
    publication_lease_expires_at timestamptz,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX ix_sr_outbox_unpublished
    ON sr_outbox_event(occurred_at, id)
    WHERE published_at IS NULL;

CREATE TABLE sr_audit_event (
    id uuid PRIMARY KEY,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id uuid NOT NULL,
    actor varchar(300) NOT NULL,
    command varchar(200) NOT NULL,
    prior_state jsonb,
    resulting_state jsonb,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX ix_sr_audit_aggregate
    ON sr_audit_event(aggregate_type, aggregate_id, occurred_at, id);
