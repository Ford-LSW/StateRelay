package com.staterelay.server.definition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.staterelay.server.persistence.OutboxRepository;
import com.staterelay.server.persistence.PostgresRepositoryTestSupport;
import com.staterelay.server.persistence.TaskInstanceRepository;
import com.staterelay.server.support.PostgresTestConfiguration;
import com.staterelay.server.trigger.ManualTriggerController;
import com.staterelay.server.trigger.TriggerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringJUnitConfig(PostgresTestConfiguration.class)
class TaskDefinitionApiTest extends PostgresRepositoryTestSupport {

    private static final Instant NOW = Instant.parse("2026-08-13T10:00:00Z");

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private ObjectMapper objectMapper;

    private TaskDefinitionController definitions;
    private ManualTriggerController triggers;
    private UUID applicationId;

    @BeforeEach
    void createApi() {
        applicationId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO sr_application(id, name) VALUES (?, ?)", applicationId, "orders");
        OutboxRepository outbox = new OutboxRepository(jdbc, objectMapper);
        TaskInstanceRepository instances = new TaskInstanceRepository(jdbc, transactions, outbox, objectMapper);
        TaskDefinitionService service = new TaskDefinitionService(jdbc, transactions, objectMapper);
        definitions = new TaskDefinitionController(service);
        triggers = new ManualTriggerController(new TriggerService(
                jdbc, instances, Clock.fixed(NOW, ZoneOffset.UTC)));
    }

    @Test
    void editingPublishedDefinitionCreatesANewVersionAndDoesNotChangeExistingSnapshot() {
        var firstDraft = definitions.create(new TaskDefinitionController.CreateDefinitionRequest(
                applicationId, "closeExpiredOrders", "closeExpiredOrders",
                JsonNodeFactory.instance.objectNode().put("timeoutSeconds", 300),
                JsonNodeFactory.instance.objectNode()));
        var firstPublished = definitions.publish(firstDraft.versionId());
        UUID instanceId = triggers.trigger(firstDraft.definitionId(),
                new ManualTriggerController.TriggerRequest(
                        "business-42", JsonNodeFactory.instance.objectNode().put("tenantId", 42))).instanceId();

        var revision = definitions.createRevision(firstDraft.definitionId(),
                new TaskDefinitionController.VersionRequest(
                        "closeExpiredOrders",
                        JsonNodeFactory.instance.objectNode().put("timeoutSeconds", 600),
                        JsonNodeFactory.instance.objectNode()));
        var secondPublished = definitions.publish(revision.versionId());

        assertThat(firstPublished.versionId()).isNotEqualTo(secondPublished.versionId());
        assertThat(jdbcTemplate.queryForObject(
                "SELECT definition_version_id FROM sr_task_instance WHERE id = ?",
                UUID.class, instanceId)).isEqualTo(firstPublished.versionId());
        assertThat(jdbcTemplate.queryForObject(
                "SELECT configuration_snapshot ->> 'timeoutSeconds' FROM sr_task_instance WHERE id = ?",
                String.class, instanceId)).isEqualTo("300");
        assertThatThrownBy(() -> jdbcTemplate.update("""
                UPDATE sr_task_definition_version
                SET configuration_snapshot = CAST(? AS jsonb)
                WHERE id = ?
                """, "{\"timeoutSeconds\":999}", firstPublished.versionId()))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void manualTriggerReturnsExistingInstanceForBusinessIdempotencyKey() {
        var draft = definitions.create(new TaskDefinitionController.CreateDefinitionRequest(
                applicationId, "invoice", "invoice",
                JsonNodeFactory.instance.objectNode(), JsonNodeFactory.instance.objectNode()));
        definitions.publish(draft.versionId());
        var request = new ManualTriggerController.TriggerRequest(
                "invoice-42", JsonNodeFactory.instance.objectNode().put("invoiceId", 42));

        UUID first = triggers.trigger(draft.definitionId(), request).instanceId();
        UUID second = triggers.trigger(draft.definitionId(), request).instanceId();

        assertThat(second).isEqualTo(first);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sr_task_instance WHERE task_definition_id = ?",
                Integer.class, draft.definitionId())).isOne();
    }

    @Test
    void manualPayloadAllowsExactly64KiBAndRejectsOneByteMore() {
        var draft = definitions.create(new TaskDefinitionController.CreateDefinitionRequest(
                applicationId, "payloadBoundary", "payloadBoundary",
                JsonNodeFactory.instance.objectNode(), JsonNodeFactory.instance.objectNode()));
        definitions.publish(draft.versionId());

        // PostgreSQL jsonb::text normalizes this object as {"data": "..."} (12 framing bytes).
        var exact = JsonNodeFactory.instance.objectNode().put("data", "x".repeat(65_524));
        var over = JsonNodeFactory.instance.objectNode().put("data", "x".repeat(65_525));

        assertThat(triggers.trigger(draft.definitionId(),
                new ManualTriggerController.TriggerRequest(null, exact)).instanceId()).isNotNull();
        assertThatThrownBy(() -> triggers.trigger(draft.definitionId(),
                new ManualTriggerController.TriggerRequest(null, over)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
