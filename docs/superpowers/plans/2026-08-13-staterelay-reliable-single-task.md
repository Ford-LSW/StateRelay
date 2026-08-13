# StateRelay Reliable Single-Task Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first production-testable StateRelay slice: a Spring Boot application registers a handler, the scheduler creates and reliably dispatches a task instance to a concrete Kubernetes Pod, and PostgreSQL-backed leases, retries, fencing, recovery, logs, and audit preserve correctness through failures.

**Architecture:** Use a Maven multi-module Java 17 project. `staterelay-contract` owns stable SDK and wire types, `staterelay-server` owns the PostgreSQL state machine and control-plane APIs, and `staterelay-spring-boot-starter` embeds registration and execution into business applications. PostgreSQL is the final source of truth; HTTP transport is retryable, Redis and a workflow engine are deliberately excluded from this first executable slice.

**Tech Stack:** Java 17, Spring Boot 3.4.13, Maven 3.9.16, PostgreSQL 16, Spring JDBC, Flyway, Jackson, Jakarta Validation, Micrometer, JUnit 5, AssertJ, Awaitility, Testcontainers 2.0.5, WireMock 3.x.

**Spec:** `docs/superpowers/specs/2026-08-12-staterelay-platform-design.md`

## Global Constraints

- First users are ordinary internal Java applications deployed with the scheduler in one Kubernetes cluster.
- Target capacity is 50 applications, 1,000,000 task instances per day, and 100 triggers per second at peak.
- Delivery semantics are at-least-once; never claim arbitrary business side effects execute exactly once.
- PostgreSQL is authoritative. No correctness decision may depend only on process memory, Redis, or an HTTP response.
- A transport retry reuses `dispatchId`; an execution retry creates a new `attemptId` and increments `attemptNo` and `leaseVersion`.
- Every result, progress, cancel, and recovery mutation must fence on `attemptId`, `leaseVersion`, `workerId`, and `workerEpoch`.
- Business payloads and results are JSON values capped at 64 KiB in this phase; large or sensitive data must be passed by reference.
- Scheduler-to-executor HTTP is instance-level Pod routing. A normal Kubernetes Service address must not represent a concrete Worker.
- Do not hold a database transaction open during an HTTP request.
- Use Spring JDBC with explicit SQL for `SKIP LOCKED`, unique constraints, capacity reservation, and compare-and-set updates; do not hide concurrency SQL behind a general ORM.
- Database integration tests run against PostgreSQL 16 through Testcontainers, not H2.
- All executor queues and scheduler dispatch pools are bounded.
- External asynchronous tasks, `RemoteAttempt`, DAG workflows, Redis/SSE, RBAC UI, broadcast, sharding, and advanced overlap policies (`SERIAL`, `DISCARD`, `REPLACE`) belong to later plans. This slice supports `PARALLEL` execution with explicit application, Handler, and task-definition concurrency limits.
- Every public SDK type has JavaDoc; internal methods have JavaDoc when their concurrency or state-transition contract is not obvious.

## File and Module Map

```text
StateRelay/
├── pom.xml                                      Maven parent and dependency management
├── mvnw, mvnw.cmd, .mvn/wrapper/*               Reproducible Maven entry point
├── staterelay-contract/                         Public handler API and wire protocol only
│   └── src/main/java/com/staterelay/contract/
├── staterelay-server/                           Control plane, PostgreSQL and HTTP APIs
│   ├── src/main/java/com/staterelay/server/
│   ├── src/main/resources/db/migration/
│   └── src/test/java/com/staterelay/server/
├── staterelay-spring-boot-starter/              Embedded Worker runtime
│   ├── src/main/java/com/staterelay/starter/
│   └── src/test/java/com/staterelay/starter/
├── staterelay-sample-order-service/              End-to-end sample business application
│   ├── src/main/java/com/staterelay/sample/
│   └── src/test/java/com/staterelay/sample/
└── docs/
    ├── superpowers/specs/                        Approved architecture
    ├── superpowers/plans/                        Executable plans
    └── operations/                               Runbook and failure procedures
```

Module dependency direction is fixed:

```text
staterelay-contract ← staterelay-server
staterelay-contract ← staterelay-spring-boot-starter ← sample-order-service
```

`staterelay-server` must never depend on Starter implementation classes, and Starter must never access scheduler tables directly.

---

### Task 1: Maven Skeleton and Stable Contract Module

**Files:**
- Create: `pom.xml`
- Create: `.gitignore`
- Create: `.mvn/wrapper/maven-wrapper.properties`
- Create: `mvnw`
- Create: `mvnw.cmd`
- Create: `staterelay-contract/pom.xml`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/handler/TaskHandler.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/handler/TaskContext.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/handler/TaskResult.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/handler/DistributedTask.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/ExecuteTaskCommand.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/DispatchAck.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/TaskResultReport.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/TaskProgressReport.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/TaskLogBatch.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/CancelTaskCommand.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/WorkerRegistrationRequest.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/WorkerHeartbeatRequest.java`
- Create: `staterelay-contract/src/main/java/com/staterelay/contract/protocol/ActiveExecutionLease.java`
- Test: `staterelay-contract/src/test/java/com/staterelay/contract/ContractSerializationTest.java`

**Interfaces:**
- Consumes: none.
- Produces: `TaskHandler<P,R>.execute(TaskContext,P)`, `TaskContext`, `TaskResult<R>`, `@DistributedTask`, `ExecuteTaskCommand`, `DispatchAck`, and `TaskResultReport` used by every later task.

- [ ] **Step 1: Initialize repository metadata and the multi-module build**

Run:

```powershell
git init
git branch -M main
mvn -N wrapper:wrapper -Dmaven=3.9.16
```

Create a parent POM with Java 17, Spring Boot `3.4.13`, Testcontainers `2.0.5`, UTF-8 encoding, compiler `-parameters`, and only `staterelay-contract` in `<modules>`. Pin the wrapper to Maven `3.9.16`. Each later task adds its module only when that module's POM is created, so every intermediate commit builds. Add `.superpowers/`, `target/`, `.idea/`, `.vscode/`, and `*.iml` to `.gitignore`.

- [ ] **Step 2: Write the failing wire-compatibility test**

```java
@Test
void executeCommandRoundTripsWithoutLosingFenceFields() throws Exception {
    ExecuteTaskCommand source = new ExecuteTaskCommand(
            "ti-1", "ta-1", 1, "dispatch-1", 7,
            "order-service", "order-service-pod-a", "epoch-a", "closeExpiredOrders",
            JsonNodeFactory.instance.objectNode().put("tenantId", 42),
            "idem-1", Duration.ofMinutes(10), Instant.parse("2026-08-13T10:00:00Z"));

    String json = objectMapper.writeValueAsString(source);
    ExecuteTaskCommand restored = objectMapper.readValue(json, ExecuteTaskCommand.class);

    assertThat(restored).isEqualTo(source);
    assertThat(restored.leaseVersion()).isEqualTo(7);
    assertThat(restored.dispatchId()).isEqualTo("dispatch-1");
    assertThat(restored.targetWorkerEpoch()).isEqualTo("epoch-a");
}
```

- [ ] **Step 3: Run the contract test and verify it fails**

Run: `./mvnw -pl staterelay-contract test -Dtest=ContractSerializationTest`

Expected: compilation fails because the contract types do not exist.

- [ ] **Step 4: Implement the minimal public contract**

Use records for wire DTOs and a sealed-style factory API for results:

```java
public interface TaskHandler<P, R> {
    TaskResult<R> execute(TaskContext context, P parameter) throws Exception;
}

public interface TaskContext {
    String taskInstanceId();
    String attemptId();
    long leaseVersion();
    String idempotencyKey();
    boolean isCancellationRequested();
    void reportProgress(int percent, String message);
}

public record TaskResult<R>(boolean success, R value, String errorCode, String message) {
    public static <R> TaskResult<R> success(R value) {
        return new TaskResult<>(true, value, null, null);
    }

    public static <R> TaskResult<R> failure(String errorCode, String message) {
        return new TaskResult<>(false, null, errorCode, message);
    }
}
```

`ExecuteTaskCommand` must contain exactly the fields used in the test. `DispatchAck` contains `dispatchId`, `attemptId`, `workerId`, `workerEpoch`, `AckStatus` (`ACCEPTED`, `DUPLICATE`, `REJECTED_CAPACITY`, `REJECTED_HANDLER`, `REJECTED_STALE_EPOCH`) and `message`. `TaskResultReport` contains instance, attempt, lease, worker ID, worker epoch, terminal status, result JSON, error code, error message, started time and finished time.

- [ ] **Step 5: Run contract tests and the module build**

Run:

```powershell
./mvnw -pl staterelay-contract test
./mvnw -pl staterelay-contract package
```

Expected: both commands pass; the contract JAR contains no Spring Boot server or JDBC dependency.

- [ ] **Step 6: Commit the stable protocol boundary**

```powershell
git add .gitignore .mvn mvnw mvnw.cmd pom.xml staterelay-contract
git commit -m "feat: establish StateRelay task contract"
```

---

### Task 2: Pure Domain State Machines and Retry Classification

**Files:**
- Modify: `pom.xml`
- Create: `staterelay-server/pom.xml`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/TaskInstanceStatus.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/TaskAttemptStatus.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/DispatchStatus.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/TaskInstanceStateMachine.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/TaskAttemptStateMachine.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/RetryPolicy.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/RetryDecision.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/domain/ErrorClass.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/domain/TaskStateMachineTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/domain/RetryPolicyTest.java`

**Interfaces:**
- Consumes: no persistence; pure values only.
- Produces: `TaskInstanceStateMachine.requireTransition(from,to)`, `TaskAttemptStateMachine.requireTransition(from,to)`, and `RetryPolicy.decide(attemptNo,errorClass,now)`.

- [ ] **Step 1: Write failing state-transition tests**

```java
@Test
void retryableAttemptFailureMovesInstanceToRetryWaitWithoutIntermediateFailedState() {
    assertThatNoException().isThrownBy(() ->
            instanceStateMachine.requireTransition(RUNNING, RETRY_WAIT));
    assertThatThrownBy(() ->
            instanceStateMachine.requireTransition(FAILED, RETRY_WAIT))
            .isInstanceOf(IllegalStateException.class);
}

@Test
void terminalAttemptCannotReturnToRunning() {
    assertThatThrownBy(() ->
            attemptStateMachine.requireTransition(SUCCESS, RUNNING))
            .isInstanceOf(IllegalStateException.class);
}
```

- [ ] **Step 2: Write the failing retry-decision test**

```java
@Test
void transientFailureUsesExponentialBackoffAndStopsAtMaxAttempts() {
    RetryPolicy policy = new RetryPolicy(3, Duration.ofSeconds(10), Duration.ofMinutes(2), 0.0);
    Instant now = Instant.parse("2026-08-13T10:00:00Z");

    assertThat(policy.decide(1, ErrorClass.TRANSIENT, now).nextRunAt())
            .isEqualTo(now.plusSeconds(10));
    assertThat(policy.decide(3, ErrorClass.TRANSIENT, now).retry()).isFalse();
    assertThat(policy.decide(1, ErrorClass.PERMANENT, now).retry()).isFalse();
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskStateMachineTest,RetryPolicyTest`

Expected: compilation fails because domain types are missing.

- [ ] **Step 4: Implement explicit transition maps and retry math**

Add `staterelay-server` to the parent POM in the same change.

Allowed instance transitions:

```text
WAITING -> READY,CANCELLED
READY -> RUNNING,CANCELLED
RUNNING -> SUCCESS,RETRY_WAIT,FAILED,CANCELLING,CANCELLED
RETRY_WAIT -> READY,CANCELLED
CANCELLING -> CANCELLED,SUCCESS,FAILED
```

Allowed attempt transitions:

```text
CREATED -> ASSIGNED,CANCELLED
ASSIGNED -> ACCEPTED,LOST,CANCELLED
ACCEPTED -> RUNNING,LOST,CANCELLED
RUNNING -> SUCCESS,FAILED,CANCELLED,LOST
```

Add `TIMED_OUT` as a terminal Task Attempt state and allow `ACCEPTED,RUNNING -> TIMED_OUT`. A timed-out Attempt can never return to RUNNING; a later report is rejected by its inactive status and fence.

Backoff formula without jitter in deterministic unit tests:

```java
Duration delay = baseDelay.multipliedBy(1L << Math.min(attemptNo - 1, 20));
delay = delay.compareTo(maxDelay) > 0 ? maxDelay : delay;
```

Production jitter is bounded to `[-jitterRatio,+jitterRatio]`; inject `DoubleSupplier` so tests use `0.5` and remain deterministic.

- [ ] **Step 5: Run pure domain tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskStateMachineTest,RetryPolicyTest`

Expected: PASS without starting Spring or PostgreSQL.

- [ ] **Step 6: Commit state semantics**

```powershell
git add staterelay-server pom.xml
git commit -m "feat: define task and attempt state machines"
```

---

### Task 3: PostgreSQL Schema, Repositories, Unique Triggering, and CAS Fencing

**Files:**
- Create: `staterelay-server/src/main/resources/db/migration/V1__reliable_single_task_schema.sql`
- Create: `staterelay-server/src/main/java/com/staterelay/server/persistence/TaskInstanceRepository.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/persistence/TaskAttemptRepository.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/persistence/DispatchRepository.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/persistence/OutboxRepository.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/support/PostgresTestConfiguration.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/persistence/MigrationSmokeTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/persistence/TaskRepositoryIntegrationTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/persistence/ConcurrentClaimIntegrationTest.java`

**Interfaces:**
- Consumes: state enums from Task 2.
- Produces: `createScheduledInstance`, `claimReadyBatch`, `createAttemptWithCapacityReservation`, `casCompleteAttempt`, `markAttemptLost`, and Outbox writes used by dispatch and recovery.

- [ ] **Step 1: Write the failing migration and repository tests**

```java
@Test
void scheduledInstanceIsUniquePerTriggerAndScheduledTime() {
    UUID first = repository.createScheduledInstance(triggerId, scheduledAt, definitionVersionId, payload);
    UUID second = repository.createScheduledInstance(triggerId, scheduledAt, definitionVersionId, payload);
    assertThat(second).isEqualTo(first);
    assertThat(repository.countByTriggerAndTime(triggerId, scheduledAt)).isOne();
}

@Test
void staleLeaseCannotCompleteAttempt() {
    AttemptFixture fixture = fixtures.runningAttempt(7);
    boolean updated = attemptRepository.casCompleteAttempt(
            fixture.attemptId(), 6, SUCCESS, JsonNodeFactory.instance.objectNode());
    assertThat(updated).isFalse();
}
```

- [ ] **Step 2: Write the failing `SKIP LOCKED` concurrency test**

Create 200 READY instances, run two claimers concurrently with batch size 100, and assert that the claimed ID sets are disjoint and their union has 200 values.

```java
assertThat(claimedByA).doesNotContainAnyElementsOf(claimedByB);
assertThat(Stream.concat(claimedByA.stream(), claimedByB.stream()).distinct()).hasSize(200);
```

- [ ] **Step 3: Run integration tests and verify failure**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskRepositoryIntegrationTest,ConcurrentClaimIntegrationTest`

Expected: Spring context or compilation failure because migration and repositories are absent.

- [ ] **Step 4: Create the V1 schema with database-enforced invariants**

Create tables: `sr_application`, `sr_worker`, `sr_task_definition`, `sr_task_definition_version`, `sr_trigger`, `sr_task_instance`, `sr_task_attempt`, `sr_dispatch`, `sr_outbox_event`, `sr_audit_event`.

Required constraints and indexes:

```sql
CREATE UNIQUE INDEX uk_sr_instance_trigger_time
    ON sr_task_instance(trigger_id, scheduled_at)
    WHERE trigger_id IS NOT NULL;

CREATE UNIQUE INDEX uk_sr_instance_business_idempotency
    ON sr_task_instance(task_definition_id, business_idempotency_key)
    WHERE business_idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX uk_sr_attempt_number
    ON sr_task_attempt(task_instance_id, attempt_no);

CREATE UNIQUE INDEX uk_sr_dispatch_id ON sr_dispatch(dispatch_id);
CREATE UNIQUE INDEX uk_sr_attempt_dispatch ON sr_dispatch(task_attempt_id);

CREATE INDEX ix_sr_instance_ready
    ON sr_task_instance(priority DESC, next_run_at, id)
    WHERE status IN ('READY', 'RETRY_WAIT');

CREATE INDEX ix_sr_attempt_expired_lease
    ON sr_task_attempt(lease_expires_at, id)
    WHERE status IN ('ASSIGNED', 'ACCEPTED', 'RUNNING');
```

Use `jsonb` for immutable configuration snapshot, payload and small result. Add check constraints for payload byte length, positive attempt numbers, progress range, and non-negative lease versions.

- [ ] **Step 5: Implement repository SQL with short transactions**

`claimReadyBatch` uses:

```sql
SELECT id
FROM sr_task_instance
WHERE status IN ('READY', 'RETRY_WAIT')
  AND next_run_at <= :now
ORDER BY priority DESC, next_run_at, id
FOR UPDATE SKIP LOCKED
LIMIT :batchSize
```

`casCompleteAttempt` updates only when `id=:attemptId AND lease_version=:leaseVersion AND status IN ('ACCEPTED','RUNNING')`. Every successful state mutation inserts an `sr_outbox_event` row in the same `TransactionTemplate` callback.

Store `current_lease_version` on `sr_task_instance`. `createAttemptWithCapacityReservation` increments it with `UPDATE ... SET current_lease_version = current_lease_version + 1 ... RETURNING current_lease_version` and copies the returned value into the new Attempt. The value is monotonic per Task Instance and is never reset or reused.

- [ ] **Step 6: Run repository and migration tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskRepositoryIntegrationTest,ConcurrentClaimIntegrationTest`

Expected: PASS against a PostgreSQL 16 Testcontainer. The concurrent test must be repeated 20 times without overlap.

- [ ] **Step 7: Validate the migration from an empty database**

Run: `./mvnw -pl staterelay-server -Dspring.flyway.clean-disabled=false test -Dtest=MigrationSmokeTest`

Expected: Flyway validates V1, all tables and partial indexes exist, and a second application start reports no pending migration.

- [ ] **Step 8: Commit persistence invariants**

```powershell
git add staterelay-server
git commit -m "feat: persist task execution state with fencing"
```

---

### Task 4: Versioned Task Definitions, API Trigger, and Due Trigger Scanner

**Files:**
- Create: `staterelay-server/src/main/java/com/staterelay/server/definition/TaskDefinitionService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/definition/TaskDefinitionController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/TriggerService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/TriggerScanner.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/CronScheduleCalculator.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/FixedRateScheduleCalculator.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/FixedDelayScheduleCoordinator.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/MisfirePolicy.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/trigger/ManualTriggerController.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/definition/TaskDefinitionApiTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/trigger/TriggerScannerIntegrationTest.java`

**Interfaces:**
- Consumes: repositories from Task 3.
- Produces: immutable published definition versions, `TriggerService.triggerNow(definitionId,idempotencyKey,payload)`, and due-trigger instance creation.

- [ ] **Step 1: Write failing definition-version test**

```java
@Test
void editingPublishedDefinitionCreatesANewVersionAndDoesNotChangeExistingSnapshot() {
    UUID v1 = client.createAndPublish(definition("closeExpiredOrders", Duration.ofMinutes(5)));
    UUID instance = client.trigger(v1, "business-42", json("tenantId", 42));
    UUID v2 = client.publishRevision(v1, definition("closeExpiredOrders", Duration.ofMinutes(10)));

    assertThat(repository.findInstance(instance).definitionVersionId()).isEqualTo(v1);
    assertThat(repository.findInstance(instance).configurationSnapshot().path("timeoutSeconds").asLong())
            .isEqualTo(300);
    assertThat(v2).isNotEqualTo(v1);
}
```

- [ ] **Step 2: Write failing concurrent trigger test**

Invoke `scanDueTriggers(now)` concurrently from two threads for the same trigger and assert one instance exists and the trigger advances once to the next fire time.

Add schedule-semantics tests:

```java
@Test
void fixedRateUsesPreviousPlannedTimeInsteadOfScannerWallClock() {
    Instant planned = Instant.parse("2026-08-13T10:00:00Z");
    assertThat(fixedRate.next(planned, Duration.ofMinutes(5)))
            .isEqualTo(Instant.parse("2026-08-13T10:05:00Z"));
}

@Test
void fixedDelaySchedulesOnlyAfterPreviousInstanceTerminates() {
    Instant completed = Instant.parse("2026-08-13T10:03:20Z");
    assertThat(fixedDelay.nextAfterCompletion(completed, Duration.ofMinutes(5)))
            .isEqualTo(Instant.parse("2026-08-13T10:08:20Z"));
}
```

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskDefinitionApiTest,TriggerScannerIntegrationTest`

Expected: FAIL because APIs and scanner are absent.

- [ ] **Step 4: Implement draft/publish and immutable snapshot creation**

Expose:

```text
POST /api/v1/task-definitions
POST /api/v1/task-definitions/{id}/versions
POST /api/v1/task-definition-versions/{versionId}/publish
POST /api/v1/task-definitions/{id}/trigger
```

Manual trigger accepts an optional caller-supplied idempotency key. Enforce `UNIQUE(definition_id, business_idempotency_key)` when the key is present, returning the existing instance on duplicate submission.

- [ ] **Step 5: Implement due-trigger scanning and exact schedule semantics**

Claim due triggers with `FOR UPDATE SKIP LOCKED`, create the instance using the current published version, compute `next_fire_at`, then commit. Support:

- `CRON`: compute from the previous scheduled time in the configured IANA time zone;
- `FIXED_RATE`: compute `previous_scheduled_at + interval`, independent of execution duration;
- `FIXED_DELAY`: keep `next_fire_at` null while an instance is active, then set `terminal_at + delay` in the same transaction that applies the terminal result;
- `ONE_TIME`: clear `next_fire_at` after the instance is created;
- API trigger: create immediately using a caller idempotency key when supplied.

Use `MisfirePolicy.FIRE_ONCE` as the first-phase default: when one or more planned occurrences were missed, create one instance with the oldest missed `scheduled_at`, then advance `next_fire_at` to the first time after `now`. Also support `SKIP`, which creates no missed instance and only advances the trigger. Do not implement unbounded catch-up in this plan.

- [ ] **Step 6: Run definition and trigger tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskDefinitionApiTest,TriggerScannerIntegrationTest`

Expected: PASS; concurrent scanners create one planned instance.

- [ ] **Step 7: Commit task creation**

```powershell
git add staterelay-server
git commit -m "feat: publish and trigger versioned task definitions"
```

---

### Task 5: Worker Registration, Heartbeat, Handler Discovery, and Kubernetes Identity

**Files:**
- Modify: `pom.xml`
- Create: `staterelay-spring-boot-starter/pom.xml`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/StateRelayAutoConfiguration.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/StateRelayProperties.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/handler/HandlerRegistry.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/registration/WorkerIdentityProvider.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/registration/WorkerRegistrationClient.java`
- Create: `staterelay-spring-boot-starter/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `staterelay-server/src/main/java/com/staterelay/server/worker/WorkerController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/worker/WorkerService.java`
- Test: `staterelay-spring-boot-starter/src/test/java/com/staterelay/starter/HandlerRegistryTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/worker/WorkerRegistrationIntegrationTest.java`

**Interfaces:**
- Consumes: `@DistributedTask`, database Worker table.
- Produces: Worker registration/heartbeat API and `HandlerRegistry.require(handlerName)` for executor dispatch.

- [ ] **Step 1: Write failing handler discovery test**

```java
@Test
void duplicateHandlerNamesFailApplicationStartup() {
    contextRunner.withBean("first", TaskHandler.class, FirstHandler::new)
            .withBean("second", TaskHandler.class, SecondHandler::new)
            .run(context -> assertThat(context).hasFailed()
                    .getFailure().hasMessageContaining("duplicate handler name: closeExpiredOrders"));
}
```

- [ ] **Step 2: Write failing worker epoch test**

Register the same `podName` twice with different `workerEpoch` values. Assert the old record becomes OFFLINE, its active Attempts become immediately eligible for fenced recovery, the new record becomes READY, and a late heartbeat from the old epoch returns HTTP 409.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-spring-boot-starter,staterelay-server -am test -Dtest=HandlerRegistryTest,WorkerRegistrationIntegrationTest`

Expected: FAIL because registration components are absent.

- [ ] **Step 4: Implement typed Starter configuration**

Add `staterelay-spring-boot-starter` to the parent POM in the same change.

```yaml
staterelay:
  enabled: true
  server-url: http://staterelay-server:8080
  app-name: order-service
  executor-port: 8080
  max-concurrency: 16
  queue-capacity: 64
  heartbeat-interval: 10s
  worker-lease: 35s
```

`WorkerIdentityProvider` reads `POD_NAME` and `POD_IP` environment variables populated through Kubernetes Downward API. `workerEpoch` is a UUID generated once per process start. Local development may use configured host and port, but production startup fails when Pod IP is absent and `environment=KUBERNETES`.

- [ ] **Step 5: Implement registration and heartbeat APIs**

```text
POST /internal/v1/workers/register
POST /internal/v1/workers/{workerId}/heartbeat
POST /internal/v1/workers/{workerId}/drain
DELETE /internal/v1/workers/{workerId}
```

Heartbeat reports Worker status, active count, queue depth, maximum concurrency, Starter version, Handler metadata, and `ActiveExecutionLease(attemptId, leaseVersion, workerEpoch)` for every locally active execution. The server sets Worker and Attempt `lease_expires_at` from its own clock. Attempt lease renewal uses a guarded update matching Attempt ID, lease version, Worker ID, Worker epoch and active status; stale epochs renew zero rows and receive HTTP 409.

Persist both `reserved_capacity` and the latest `reported_active_count`. Routing uses `effective_load = max(reserved_capacity, reported_active_count)`, so a timed-out but still-running cooperative Java task cannot make the Worker appear to have free capacity.

- [ ] **Step 6: Run registration tests**

Run: `./mvnw -pl staterelay-spring-boot-starter,staterelay-server -am test -Dtest=HandlerRegistryTest,WorkerRegistrationIntegrationTest`

Expected: PASS; duplicate handlers stop startup and stale epochs cannot revive old Workers.

- [ ] **Step 7: Commit Worker discovery**

```powershell
git add staterelay-spring-boot-starter staterelay-server pom.xml
git commit -m "feat: register worker pods and task handlers"
```

---

### Task 6: Capacity Reservation, Worker Routing, and Reliable HTTP Dispatch

**Files:**
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/WorkerRouter.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/PowerOfTwoChoicesRouter.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/CapacityReservationService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/DispatchService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/ExecutorHttpClient.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/dispatch/DispatchScanner.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/dispatch/WorkerRouterTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/dispatch/DispatchServiceIntegrationTest.java`

**Interfaces:**
- Consumes: READY instances, Worker registry, `ExecuteTaskCommand`.
- Produces: atomic capacity reservation, Attempt/Dispatch creation, and transport retry using the same `dispatchId`.

- [ ] **Step 1: Write failing routing test**

```java
@Test
void routerNeverChoosesDrainingExpiredIncompatibleOrFullWorker() {
    List<WorkerCandidate> candidates = List.of(
            worker("a", READY, 3, 16, compatible()),
            worker("b", DRAINING, 0, 16, compatible()),
            worker("c", READY, 0, 16, incompatible()),
            worker("d", READY, 16, 16, compatible()));

    assertThat(router.choose(candidates, requirements)).get()
            .extracting(WorkerCandidate::workerId).isEqualTo("a");
}
```

- [ ] **Step 2: Write failing ACK-loss test with WireMock**

Configure the executor stub to accept the first POST and close the connection before returning; the second POST returns DUPLICATE to state that the same dispatch was already accepted. Capture request bodies and assert both contain the same `dispatchId` and `attemptId`, while the database contains one Attempt and one Dispatch.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-server -am test -Dtest=WorkerRouterTest,DispatchServiceIntegrationTest`

Expected: FAIL because routing and dispatch do not exist.

- [ ] **Step 4: Implement atomic reservation**

Within one short transaction:

1. lock the selected Worker row;
2. verify READY, lease valid, Handler compatible, and `reserved_capacity < max_concurrency`;
3. verify `max(reserved_capacity, reported_active_count) < max_concurrency` and increment reserved capacity;
4. create Attempt with incremented `attemptNo` and `leaseVersion`;
5. create one Dispatch with a generated `dispatchId`;
6. move Task Instance to RUNNING and write Outbox.

If the conditional Worker update affects zero rows, select another Worker; never overbook based on heartbeat cache alone.

- [ ] **Step 5: Implement HTTP dispatch outside the transaction**

`ExecutorHttpClient.execute(workerAddress, command)` has connect timeout 1 second and response timeout 3 seconds. On timeout, update Dispatch to UNCERTAIN and schedule transport retry with the same IDs. On `REJECTED_CAPACITY`, release reservation and create a new Attempt only after the original Worker explicitly confirms the dispatch was not accepted.

Treat both ACCEPTED and DUPLICATE ACKs as proof that the logical dispatch is accepted. On an explicit REJECTED response, close the unaccepted Attempt as LOST, release its capacity once, and let the Dispatcher create a new Attempt with a higher lease version for another Worker; never mutate the owning Worker of an existing Attempt.

- [ ] **Step 6: Run routing and dispatch tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=WorkerRouterTest,DispatchServiceIntegrationTest`

Expected: PASS; simulated ACK loss produces two HTTP transmissions but one logical Dispatch and one Attempt.

- [ ] **Step 7: Commit reliable dispatch**

```powershell
git add staterelay-server
git commit -m "feat: reserve worker capacity and dispatch reliably"
```

---

### Task 7: Starter Executor Endpoint, Durable Dispatch Deduplication, and Bounded Execution

**Files:**
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/ExecutorController.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/DispatchDeduplicator.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/ExecutionCoordinator.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/DefaultTaskContext.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/ResultReporter.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/LocalDispatchStore.java`
- Test: `staterelay-spring-boot-starter/src/test/java/com/staterelay/starter/execution/ExecutorIdempotencyTest.java`
- Test: `staterelay-spring-boot-starter/src/test/java/com/staterelay/starter/execution/BoundedExecutorTest.java`

**Interfaces:**
- Consumes: `ExecuteTaskCommand`, Handler registry, registration identity.
- Produces: `POST /staterelay/internal/v1/executions`, one-time local admission per dispatch, bounded execution, progress and terminal reporting.

- [ ] **Step 1: Write failing duplicate dispatch test**

Send the same command twice while the first run is blocked on a latch:

```java
assertThat(firstAck.status()).isEqualTo(ACCEPTED);
assertThat(secondAck.status()).isEqualTo(DUPLICATE);
assertThat(handlerInvocationCount).hasValue(1);
```

Restart the Starter application context using the same local store directory. Because process start creates a new `workerEpoch`, send the old command and assert `REJECTED_STALE_EPOCH`; the Handler must not be invoked. The scheduler then closes the old Attempt as LOST and creates a new Attempt and dispatch for the new epoch.

- [ ] **Step 2: Write failing bounded queue test**

Configure `maxConcurrency=1` and `queueCapacity=1`. Block the first Handler, queue a second command, and assert the third returns `REJECTED_CAPACITY` within 100 ms without entering the queue.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-spring-boot-starter -am test -Dtest=ExecutorIdempotencyTest,BoundedExecutorTest`

Expected: FAIL because the executor runtime is missing.

- [ ] **Step 4: Implement local dispatch journal before ACK**

`LocalDispatchStore` stores compact records under `${staterelay.work-dir}/dispatch/` using atomic temp-file rename and fsync before returning ACCEPTED. Record fields: dispatchId, attemptId, leaseVersion, state (`ACCEPTED`, `RUNNING`, `TERMINAL`), acceptedAt and terminal report checksum. Prune terminal records only after the scheduler confirms receipt and the configured retention period elapses.

The command target Worker ID and epoch must match the local process before capacity reservation or journal writes. This journal makes an ACK truthful within one Worker epoch and allows a restarted process to recognize but reject stale-epoch commands. Do not return ACCEPTED based only on an in-memory map, and do not automatically re-execute nonterminal journal records under a new epoch.

- [ ] **Step 5: Implement bounded execution and Handler invocation**

Use `ThreadPoolTaskExecutor` with exact core/max concurrency from configuration, bounded queue capacity, named threads, and `AbortPolicy`. Deserialize payload using the Handler parameter Schema. Persist RUNNING to the local journal before invoking the Handler. Catch all Handler exceptions and convert them into `TaskResultReport` without killing the Worker thread.

- [ ] **Step 6: Implement progress and result reporting**

Coalesce progress to at most one report per task per second. Terminal reports retry with exponential backoff and remain in the local journal until the scheduler acknowledges them. Every report carries `attemptId` and `leaseVersion`.

- [ ] **Step 7: Run Starter execution tests**

Run: `./mvnw -pl staterelay-spring-boot-starter -am test -Dtest=ExecutorIdempotencyTest,BoundedExecutorTest`

Expected: PASS across duplicate concurrent requests and a simulated Starter restart.

- [ ] **Step 8: Commit embedded executor**

```powershell
git add staterelay-spring-boot-starter
git commit -m "feat: execute handlers with durable dispatch deduplication"
```

---

### Task 8: Result Callback, Lease Fencing, Retry Scheduling, and Capacity Release

**Files:**
- Create: `staterelay-server/src/main/java/com/staterelay/server/result/TaskResultController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/result/TaskResultService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/result/ErrorClassifier.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/result/ProgressController.java`
- Modify: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/ResultReporter.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/result/TaskResultServiceIntegrationTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/result/StaleResultFencingTest.java`

**Interfaces:**
- Consumes: `TaskResultReport`, state machines, retry policy and repositories.
- Produces: idempotent terminal acknowledgement, retry scheduling, terminal instance state and exactly-once capacity release in the platform database.

- [ ] **Step 1: Write failing stale-result test**

Create Attempt 1 with lease 7, mark it LOST, create Attempt 2 with lease 8, then submit SUCCESS for Attempt 1:

```java
assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT);
assertThat(instanceRepository.find(instanceId).status()).isEqualTo(RUNNING);
assertThat(workerRepository.reservedCapacity(workerForAttempt2)).isEqualTo(1);
```

- [ ] **Step 2: Write failing retry test**

Report a TRANSIENT failure for Attempt 1 and assert Attempt is FAILED, Task Instance is RETRY_WAIT, `next_run_at` equals policy output, capacity returns to zero, and one Outbox event records the transition. Report the same failure again and assert no counters or events change.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskResultServiceIntegrationTest,StaleResultFencingTest`

Expected: FAIL because result APIs are absent.

- [ ] **Step 4: Implement one transactional result decision**

Within a single transaction:

1. lock Attempt by ID;
2. compare taskInstanceId, leaseVersion, Worker ID and Worker epoch;
3. return the stored acknowledgement if already terminal with the same report checksum;
4. reject mismatched or stale reports with HTTP 409;
5. transition Attempt to SUCCESS or FAILED;
6. release reserved capacity using a guarded `capacity_released_at IS NULL` update;
7. transition Instance to SUCCESS, RETRY_WAIT, or FAILED;
8. insert Outbox event.

- [ ] **Step 5: Implement error classification boundary**

Classify only platform-known failures automatically: HTTP 429/502/503/504, connection reset and configured SQL transient classes are TRANSIENT; missing Handler, validation failure and explicit business rejection are PERMANENT. Unknown Handler exceptions default to PERMANENT unless the task definition explicitly lists their error code as retryable.

- [ ] **Step 6: Run result and fencing tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=TaskResultServiceIntegrationTest,StaleResultFencingTest`

Expected: PASS; a late result never changes the active Attempt, and duplicate callbacks do not double-release capacity.

- [ ] **Step 7: Commit result processing**

```powershell
git add staterelay-server staterelay-spring-boot-starter
git commit -m "feat: fence results and schedule execution retries"
```

---

### Task 9: Lease Recovery, Cooperative Cancellation, and Graceful Draining

**Files:**
- Create: `staterelay-server/src/main/java/com/staterelay/server/recovery/LeaseRecoveryScanner.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/recovery/ExecutionTimeoutScanner.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/cancel/TaskCancellationService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/cancel/TaskCancellationController.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/CancellationRegistry.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/lifecycle/GracefulDrainManager.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/execution/ExecutorControlController.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/recovery/LeaseRecoveryIntegrationTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/recovery/ExecutionTimeoutIntegrationTest.java`
- Test: `staterelay-spring-boot-starter/src/test/java/com/staterelay/starter/lifecycle/GracefulDrainTest.java`

**Interfaces:**
- Consumes: active Attempt lease, Worker lifecycle and `TaskContext.isCancellationRequested()`.
- Produces: expired lease recovery, cancellation commands and READY → DRAINING → OFFLINE lifecycle.

- [ ] **Step 1: Write failing lease recovery test**

Create an expired RUNNING Attempt and run two recovery scanners concurrently. Assert exactly one scanner marks it LOST, capacity is released once, and the Instance enters RETRY_WAIT with one recovery event.

- [ ] **Step 2: Write failing graceful drain test**

Begin one blocked task, call `drain()`, then send a second command. Assert the second command returns REJECTED_CAPACITY, the first task can finish and report SUCCESS, and the Worker deregisters only after active count reaches zero or grace time expires.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-server,staterelay-spring-boot-starter -am test -Dtest=LeaseRecoveryIntegrationTest,ExecutionTimeoutIntegrationTest,GracefulDrainTest`

Expected: FAIL because recovery and drain components are absent.

- [ ] **Step 4: Implement fenced recovery**

Claim expired active Attempts using `FOR UPDATE SKIP LOCKED`. The update must include the observed lease version and active status. Mark Attempt LOST, release capacity once, and move the Instance to RETRY_WAIT or FAILED according to retry policy. Never reuse a LOST Attempt; the Dispatcher creates the next Attempt and increments the lease version.

- [ ] **Step 5: Implement cooperative cancellation**

Expose `POST /api/v1/task-instances/{id}/cancel`. Transition Instance to CANCELLING and send `POST /staterelay/internal/v1/executions/{attemptId}/cancel` to the owning Pod. Starter flips the cancellation flag used by `TaskContext`. It may interrupt only Starter-owned waits; it must not call `Thread.stop()` or claim that arbitrary business code has stopped.

`ExecutionTimeoutScanner` separately claims Attempts whose `started_at + timeout < now`. It marks the Attempt TIMED_OUT with the full fence, invalidates its lease, schedules retry or final failure, and sends the same cooperative cancel command. Logical reserved capacity is released once, but routing continues to honor the Worker's `reported_active_count` until the old Handler really exits.

- [ ] **Step 6: Implement Kubernetes drain hooks**

On Spring `ContextClosedEvent` and an explicit internal drain endpoint:

1. set local state DRAINING before unregistering;
2. heartbeat DRAINING immediately;
3. reject new dispatches;
4. wait up to `staterelay.drain-timeout` for active tasks;
5. flush terminal reports;
6. deregister Worker.

Document a Kubernetes `preStop` HTTP call and set `terminationGracePeriodSeconds` greater than drain timeout plus report flush timeout.

- [ ] **Step 7: Run recovery and drain tests**

Run: `./mvnw -pl staterelay-server,staterelay-spring-boot-starter -am test -Dtest=LeaseRecoveryIntegrationTest,ExecutionTimeoutIntegrationTest,GracefulDrainTest`

Expected: PASS; concurrent recovery has one winner, and draining accepts no new work.

- [ ] **Step 8: Commit recovery controls**

```powershell
git add staterelay-server staterelay-spring-boot-starter
git commit -m "feat: recover lost leases and drain workers safely"
```

---

### Task 10: Outbox Relay, Metrics, Audit, and Instance Timeline API

**Files:**
- Create: `staterelay-server/src/main/java/com/staterelay/server/outbox/OutboxRelay.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/observability/StateRelayMetrics.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/timeline/InstanceTimelineController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/timeline/InstanceTimelineQuery.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/instance/TaskInstanceQueryController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/instance/TaskInstanceQueryService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/instance/TaskInstanceCommandController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/instance/TaskInstanceCommandService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/audit/AuditService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/audit/AuditEvent.java`
- Create: `staterelay-server/src/main/resources/db/migration/V2__task_log_chunks.sql`
- Create: `staterelay-server/src/main/java/com/staterelay/server/log/TaskLogController.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/log/TaskLogService.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/log/TaskLogRepository.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/log/TaskLogReporter.java`
- Create: `docs/operations/instance-recovery-runbook.md`
- Test: `staterelay-server/src/test/java/com/staterelay/server/outbox/OutboxRelayIntegrationTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/timeline/InstanceTimelineApiTest.java`
- Test: `staterelay-server/src/test/java/com/staterelay/server/log/TaskLogIngestionTest.java`

**Interfaces:**
- Consumes: Outbox and all state records.
- Produces: at-least-once local event relay, Micrometer metrics, unified instance evidence chain, and audited manual actions.

- [ ] **Step 1: Write failing Outbox crash-recovery test**

Publish an event to a fake sink, simulate process failure before setting `published_at`, restart relay, and assert the sink receives the same `eventId` again while the state mutation is never lost. The sink fixture deduplicates on `eventId`.

- [ ] **Step 2: Write failing timeline API test**

Create an instance with two Attempts, an UNCERTAIN then ACKED Dispatch, a retry event and a terminal result. Assert `GET /api/v1/task-instances/{id}/timeline` returns chronologically ordered items with instanceId, attemptId, dispatchId, workerId, leaseVersion, event type and summary.

- [ ] **Step 3: Run tests and verify failure**

Run: `./mvnw -pl staterelay-server -am test -Dtest=OutboxRelayIntegrationTest,InstanceTimelineApiTest`

Expected: FAIL because relay and query endpoints are absent.

- [ ] **Step 4: Implement Outbox polling and publication contract**

Claim unpublished events with `FOR UPDATE SKIP LOCKED LIMIT 100`, mark a short publication lease, commit, publish outside the transaction, then CAS `published_at`. A crash can duplicate publication, so every event has a stable UUID and all consumers must deduplicate.

The initial sink writes structured application events and invokes alert evaluators in-process. Redis and Kafka adapters are not added in this phase.

- [ ] **Step 5: Add required metrics**

Register Micrometer meters:

```text
staterelay.trigger.lag
staterelay.instance.backlog{status}
staterelay.dispatch.ack.duration
staterelay.dispatch.retransmit
staterelay.attempt.result{status,error_class}
staterelay.worker.capacity{app,worker}
staterelay.lease.recovered
staterelay.outbox.age
```

Do not use taskInstanceId, attemptId or workerId as unbounded metric tags; those belong in logs and traces.

- [ ] **Step 6: Implement timeline and audited actions**

Expose `GET /api/v1/task-instances` with application, definition, status, scheduled-time range, error class and opaque cursor filters. The sort key is `(scheduled_at DESC, id DESC)`; do not use deep offset pagination. Timeline is a read model assembled from Instance, Attempt, Dispatch, Outbox and Audit rows using cursor pagination.

Expose CAS-backed commands for retry, cancel, pause-definition and resume-definition. A manual retry is permitted only from FAILED or CANCELLED, creates a READY transition and later a new Attempt, and never reopens an old Attempt. Every command requires a non-empty reason and writes actor, command, prior state, resulting state and timestamp.

Add `sr_task_log_chunk` in a V2 Flyway migration with `(attempt_id, sequence_no)` uniqueness. Starter batches UTF-8 log lines up to 64 KiB or one second, redacts configured keys, and retries `TaskLogBatch` with the same sequence number. The server acknowledges duplicate chunks without storing them twice. Timeline returns log metadata and a cursor URL rather than embedding all log text.

- [ ] **Step 7: Write the operational runbook**

Document exact procedures for ACK UNCERTAIN, expired lease, RETRY_WAIT backlog, stale Worker, duplicate callback, Outbox backlog and a forced terminal decision. Every procedure uses API commands and verification queries; none instruct operators to update state columns directly.

- [ ] **Step 8: Run observability tests**

Run: `./mvnw -pl staterelay-server -am test -Dtest=OutboxRelayIntegrationTest,InstanceTimelineApiTest`

Expected: PASS; duplicate event delivery uses the same event ID and timeline order is deterministic.

- [ ] **Step 9: Commit operational evidence chain**

```powershell
git add staterelay-server docs/operations
git commit -m "feat: expose task timeline and recovery metrics"
```

---

### Task 11: End-to-End Sample, Fault Injection, Security Boundary, and Capacity Baseline

**Files:**
- Modify: `pom.xml`
- Create: `staterelay-sample-order-service/pom.xml`
- Create: `staterelay-sample-order-service/src/main/java/com/staterelay/sample/SampleOrderApplication.java`
- Create: `staterelay-sample-order-service/src/main/java/com/staterelay/sample/CloseExpiredOrdersTask.java`
- Create: `staterelay-sample-order-service/src/main/java/com/staterelay/sample/OrderService.java`
- Create: `staterelay-sample-order-service/src/main/java/com/staterelay/sample/CloseRequest.java`
- Create: `staterelay-sample-order-service/src/main/java/com/staterelay/sample/CloseResult.java`
- Create: `staterelay-sample-order-service/src/main/resources/application.yml`
- Create: `staterelay-sample-order-service/src/test/java/com/staterelay/sample/ReliableDispatchEndToEndTest.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/e2e/SchedulerCrashRecoveryTest.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/e2e/AckLossEndToEndTest.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/e2e/WorkerCrashRecoveryTest.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/security/InternalApiAuthenticationTest.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/security/ServiceIdentityVerifier.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/security/HmacServiceIdentityVerifier.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/security/ServiceRequestSigner.java`
- Create: `staterelay-server/src/main/java/com/staterelay/server/security/HmacServiceRequestSigner.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/security/ServiceRequestSigner.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/security/ServiceRequestVerifier.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/security/HmacServiceRequestSigner.java`
- Create: `staterelay-spring-boot-starter/src/main/java/com/staterelay/starter/security/HmacServiceRequestVerifier.java`
- Create: `staterelay-server/src/test/java/com/staterelay/server/performance/DispatchCapacityBenchmark.java`
- Create: `docs/operations/kubernetes-deployment-example.yaml`
- Create: `docs/operations/capacity-baseline.md`
- Create: `README.md`

**Interfaces:**
- Consumes: the full reliable single-task loop.
- Produces: one runnable sample, automated failure evidence, internal API authentication, Kubernetes deployment reference and a repeatable baseline report.

- [ ] **Step 1: Write the failing happy-path end-to-end test**

Start PostgreSQL, scheduler and sample application on random ports. Wait for Handler registration, publish a definition, trigger it through the public API, and await SUCCESS. Assert one Handler invocation, one Attempt, one Dispatch and one terminal timeline event.

- [ ] **Step 2: Write failing failure-injection tests**

Automate three required failures:

1. drop the first ACK response and verify same-dispatch retransmission;
2. stop the active Worker after RUNNING, advance the test clock past lease expiry, start a replacement Worker and verify old result fencing;
3. stop a scheduler instance after committing an instance, start another scheduler and verify it dispatches the existing instance without creating a duplicate.

- [ ] **Step 3: Write failing internal API authentication tests**

Assert missing or invalid service token receives HTTP 401 on Worker registration, executor dispatch and result callback. Assert a token scoped to `order-service` cannot report a result for another application. Keep authentication behind a `ServiceIdentityVerifier` interface so mTLS or workload identity can replace the first HMAC implementation.

- [ ] **Step 4: Run end-to-end tests and verify failure**

Run: `./mvnw verify -Pe2e`

Expected: FAIL until the sample wiring, service authentication and failure controls are complete.

- [ ] **Step 5: Implement the sample Handler and Kubernetes reference**

Add `staterelay-sample-order-service` to the parent POM in the same change.

```java
@Component
@DistributedTask("closeExpiredOrders")
final class CloseExpiredOrdersTask implements TaskHandler<CloseRequest, CloseResult> {
    private final OrderService orderService;

    @Override
    public TaskResult<CloseResult> execute(TaskContext context, CloseRequest request) {
        CloseResult result = orderService.closeExpiredOrders(request.tenantId(), context.idempotencyKey());
        return TaskResult.success(result);
    }
}
```

The Kubernetes example includes Downward API environment values for Pod name/IP, readiness probe that requires Worker READY, `preStop` drain call, NetworkPolicy, PodDisruptionBudget and `terminationGracePeriodSeconds` greater than configured drain timeout.

- [ ] **Step 6: Implement first internal service identity verifier**

Use HMAC-signed requests in both directions with key ID, timestamp, nonce, HTTP method, path and SHA-256 body digest. Reject timestamps outside 60 seconds and duplicate nonce/key pairs during the replay window. Scheduler signs dispatch and cancel commands; Starter verifies them. Starter signs registration, heartbeat, progress, log and result reports; scheduler verifies them. Add a shared test vector string and assert both implementations produce the same Base64 signature. Secrets come from Kubernetes Secret mounts or environment references and are never written to logs.

- [ ] **Step 7: Run the complete correctness suite**

Run:

```powershell
./mvnw clean verify -Pe2e
```

Expected: all unit, PostgreSQL integration, Starter context and end-to-end failure tests pass. Test output must show zero duplicate planned instances and zero accepted stale-lease results.

- [ ] **Step 8: Run the capacity benchmark**

Benchmark configuration:

```text
50 logical applications
200 registered Workers
1,000,000 pre-generated instances distributed over a synthetic day
100 new due triggers/second for 30 minutes
3 scheduler replicas
PostgreSQL 16 with recorded CPU, memory, storage and connection limits
No Handler business delay for dispatch throughput measurement
```

Run: `./mvnw -pl staterelay-server -Pbenchmark test -Dtest=DispatchCapacityBenchmark`

Record in `docs/operations/capacity-baseline.md`: environment, commit, database settings, P50/P95/P99 schedule-to-dispatch latency, database CPU, lock wait, query plans, retransmit rate and backlog age. Acceptance targets are P95 ≤ 1 second and P99 ≤ 3 seconds when Worker capacity is available.

- [ ] **Step 9: Document local quick start and operational limits**

README must show: start PostgreSQL, start scheduler, start sample Worker, create a definition, trigger a task, inspect timeline, cancel, and drain. State clearly that the phase provides at-least-once execution and requires business idempotency.

- [ ] **Step 10: Run final verification and inspect repository state**

Run:

```powershell
./mvnw clean verify -Pe2e
git status --short
git diff --check
```

Expected: all tests pass, `git diff --check` prints nothing, and only intended documentation or benchmark output remains uncommitted.

- [ ] **Step 11: Commit the first production-testable slice**

```powershell
git add README.md docs staterelay-sample-order-service staterelay-server staterelay-spring-boot-starter pom.xml
git commit -m "feat: complete reliable single-task execution loop"
```

---

## Follow-on Plans

After this plan passes its correctness and capacity gates, create separate implementation plans in this order:

1. `StateRelay External Async Task Protocol`: `RemoteAttempt`, `requestId`, UNKNOWN reconciliation, callback/polling fusion, remote cancellation and result references, validated with the GIS adapter.
2. `StateRelay Simple Workflow Engine`: versioned workflow definitions, immutable node snapshots, ALL_SUCCESS/ALL_DONE/ANY_SUCCESS/EXPRESSION rules, parameter references and workflow timeline.
3. `StateRelay Execution Modes and Enterprise Governance`: SERIAL/DISCARD/REPLACE overlap semantics, application-level RBAC, alert routing, retention/partition management, broadcast, static sharding and administrative UI.

Each follow-on plan must depend on the public contracts and state invariants established here rather than bypassing them.
