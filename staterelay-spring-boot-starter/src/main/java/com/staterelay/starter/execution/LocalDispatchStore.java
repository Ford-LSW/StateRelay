package com.staterelay.starter.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.staterelay.contract.protocol.ExecuteTaskCommand;
import com.staterelay.contract.protocol.TaskResultReport;
import com.staterelay.starter.registration.WorkerIdentityProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public final class LocalDispatchStore {

    private static final int FORMAT_VERSION = 1;

    private final Path dispatchDirectory;
    private final ObjectMapper objectMapper;

    public LocalDispatchStore(Path workDirectory, ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        Objects.requireNonNull(workDirectory, "workDirectory");
        dispatchDirectory = workDirectory.toAbsolutePath().normalize().resolve("dispatch");
        try {
            Files.createDirectories(dispatchDirectory);
            validateExistingRecords();
        } catch (IOException exception) {
            throw new UncheckedIOException("cannot initialize local dispatch store", exception);
        }
    }

    public synchronized boolean createAccepted(
            ExecuteTaskCommand command,
            WorkerIdentityProvider.WorkerIdentity identity,
            Instant acceptedAt) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(identity, "identity");
        Path target = recordPath(command.dispatchId());
        if (Files.exists(target)) {
            return false;
        }
        DispatchRecord record = new DispatchRecord(
                FORMAT_VERSION, command.dispatchId(), command.attemptId(),
                command.taskInstanceId(), command.leaseVersion(),
                identity.workerId().toString(), identity.workerEpoch().toString(),
                DispatchState.ACCEPTED, Objects.requireNonNull(acceptedAt, "acceptedAt"),
                null, null, null, null, 0, null, null);
        writeRecord(target, record, false);
        return true;
    }

    public synchronized DispatchRecord markRunning(String dispatchId, Instant startedAt) {
        DispatchRecord current = require(dispatchId);
        if (current.state() != DispatchState.ACCEPTED) {
            throw new IllegalStateException("dispatch is not ACCEPTED: " + dispatchId);
        }
        DispatchRecord running = new DispatchRecord(
                current.formatVersion(), current.dispatchId(), current.attemptId(),
                current.taskInstanceId(), current.leaseVersion(), current.workerId(),
                current.workerEpoch(), DispatchState.RUNNING, current.acceptedAt(),
                Objects.requireNonNull(startedAt, "startedAt"), null, null, null, 0,
                null, null);
        writeRecord(recordPath(dispatchId), running, true);
        return running;
    }

    public synchronized DispatchRecord markTerminal(
            String dispatchId,
            TaskResultReport report,
            String checksum,
            Instant terminalAt) {
        DispatchRecord current = require(dispatchId);
        if (current.state() != DispatchState.RUNNING) {
            throw new IllegalStateException("dispatch is not RUNNING: " + dispatchId);
        }
        if (!checksum(objectMapper, report).equals(checksum)) {
            throw new IllegalArgumentException("terminal report checksum does not match report");
        }
        DispatchRecord terminal = new DispatchRecord(
                current.formatVersion(), current.dispatchId(), current.attemptId(),
                current.taskInstanceId(), current.leaseVersion(), current.workerId(),
                current.workerEpoch(), DispatchState.TERMINAL, current.acceptedAt(),
                current.startedAt(), Objects.requireNonNull(terminalAt, "terminalAt"),
                Objects.requireNonNull(report, "report"), checksum, 0, terminalAt, null);
        writeRecord(recordPath(dispatchId), terminal, true);
        return terminal;
    }

    public synchronized DispatchRecord recordTerminalDeliveryFailure(
            String dispatchId, Instant nextAttemptAt) {
        DispatchRecord current = requireTerminal(dispatchId);
        if (current.acknowledgedAt() != null) {
            return current;
        }
        DispatchRecord retry = new DispatchRecord(
                current.formatVersion(), current.dispatchId(), current.attemptId(),
                current.taskInstanceId(), current.leaseVersion(), current.workerId(),
                current.workerEpoch(), current.state(), current.acceptedAt(),
                current.startedAt(), current.terminalAt(), current.terminalReport(),
                current.terminalChecksum(), current.terminalDeliveryAttempts() + 1,
                Objects.requireNonNull(nextAttemptAt, "nextAttemptAt"), null);
        writeRecord(recordPath(dispatchId), retry, true);
        return retry;
    }

    public synchronized DispatchRecord markTerminalAcknowledged(
            String dispatchId, Instant acknowledgedAt) {
        DispatchRecord current = requireTerminal(dispatchId);
        if (current.acknowledgedAt() != null) {
            return current;
        }
        DispatchRecord acknowledged = new DispatchRecord(
                current.formatVersion(), current.dispatchId(), current.attemptId(),
                current.taskInstanceId(), current.leaseVersion(), current.workerId(),
                current.workerEpoch(), current.state(), current.acceptedAt(),
                current.startedAt(), current.terminalAt(), current.terminalReport(),
                current.terminalChecksum(), current.terminalDeliveryAttempts(),
                current.nextTerminalDeliveryAt(),
                Objects.requireNonNull(acknowledgedAt, "acknowledgedAt"));
        writeRecord(recordPath(dispatchId), acknowledged, true);
        return acknowledged;
    }

    public synchronized Optional<DispatchRecord> find(String dispatchId) {
        Path path = recordPath(dispatchId);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        return Optional.of(readRecord(path));
    }

    public synchronized DispatchRecord require(String dispatchId) {
        return find(dispatchId).orElseThrow(() ->
                new IllegalStateException("unknown local dispatch: " + dispatchId));
    }

    public synchronized List<DispatchRecord> records() {
        try (Stream<Path> files = Files.list(dispatchDirectory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".json"))
                    .map(this::readRecord)
                    .sorted(Comparator.comparing(DispatchRecord::acceptedAt)
                            .thenComparing(DispatchRecord::dispatchId))
                    .toList();
        } catch (IOException exception) {
            throw new UncheckedIOException("cannot list local dispatch records", exception);
        }
    }

    public synchronized void remove(String dispatchId) {
        try {
            if (Files.deleteIfExists(recordPath(dispatchId))) {
                forceDirectory();
            }
        } catch (IOException exception) {
            throw new UncheckedIOException("cannot remove local dispatch record", exception);
        }
    }

    public synchronized int pruneAcknowledged(Duration retention, Instant now) {
        if (retention.isNegative()) {
            throw new IllegalArgumentException("terminal retention must not be negative");
        }
        int removed = 0;
        for (DispatchRecord record : records()) {
            if (record.state() == DispatchState.TERMINAL
                    && record.acknowledgedAt() != null
                    && !record.acknowledgedAt().plus(retention).isAfter(now)) {
                remove(record.dispatchId());
                removed++;
            }
        }
        return removed;
    }

    public static String checksum(ObjectMapper objectMapper, TaskResultReport report) {
        try {
            return sha256(objectMapper.writeValueAsBytes(report));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("cannot serialize terminal report", exception);
        }
    }

    private void validateExistingRecords() throws IOException {
        try (Stream<Path> files = Files.list(dispatchDirectory)) {
            for (Path path : files.filter(file ->
                    file.getFileName().toString().endsWith(".json")).toList()) {
                readRecord(path);
            }
        }
    }

    private DispatchRecord requireTerminal(String dispatchId) {
        DispatchRecord current = require(dispatchId);
        if (current.state() != DispatchState.TERMINAL) {
            throw new IllegalStateException("dispatch is not TERMINAL: " + dispatchId);
        }
        return current;
    }

    private DispatchRecord readRecord(Path path) {
        try {
            DispatchRecord record = objectMapper.readValue(path.toFile(), DispatchRecord.class);
            validateRecord(path, record);
            return record;
        } catch (IOException | RuntimeException exception) {
            throw new IllegalStateException(
                    "invalid local dispatch record: " + path.getFileName(), exception);
        }
    }

    private void validateRecord(Path path, DispatchRecord record) {
        Objects.requireNonNull(record, "record");
        if (record.formatVersion() != FORMAT_VERSION
                || isBlank(record.dispatchId())
                || isBlank(record.attemptId())
                || isBlank(record.taskInstanceId())
                || record.leaseVersion() < 0
                || isBlank(record.workerId())
                || isBlank(record.workerEpoch())
                || record.state() == null
                || record.acceptedAt() == null) {
            throw new IllegalArgumentException("missing required dispatch fields");
        }
        if (!path.getFileName().toString().equals(fileName(record.dispatchId()))) {
            throw new IllegalArgumentException("dispatch filename does not match record");
        }
        if (record.state() != DispatchState.ACCEPTED && record.startedAt() == null) {
            throw new IllegalArgumentException("RUNNING or TERMINAL record has no startedAt");
        }
        if (record.state() == DispatchState.TERMINAL) {
            if (record.terminalAt() == null || record.terminalReport() == null
                    || isBlank(record.terminalChecksum())
                    || record.nextTerminalDeliveryAt() == null
                    || !checksum(objectMapper, record.terminalReport())
                            .equals(record.terminalChecksum())) {
                throw new IllegalArgumentException("invalid terminal record");
            }
        }
    }

    private void writeRecord(Path target, DispatchRecord record, boolean replace) {
        Path temporary = dispatchDirectory.resolve(
                target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(record);
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            if (replace) {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            }
            forceDirectory();
        } catch (AtomicMoveNotSupportedException exception) {
            throw new IllegalStateException(
                    "local dispatch filesystem does not support atomic rename", exception);
        } catch (IOException exception) {
            throw new UncheckedIOException("cannot persist local dispatch record", exception);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException ignored) {
                // The final record, not an abandoned temp file, is the source of truth.
            }
        }
    }

    private void forceDirectory() {
        try (FileChannel directory = FileChannel.open(dispatchDirectory, StandardOpenOption.READ)) {
            directory.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Directory fsync is best effort on filesystems that do not expose it.
        }
    }

    private Path recordPath(String dispatchId) {
        if (isBlank(dispatchId)) {
            throw new IllegalArgumentException("dispatchId must not be blank");
        }
        return dispatchDirectory.resolve(fileName(dispatchId));
    }

    private static String fileName(String dispatchId) {
        return sha256(dispatchId.getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".json";
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public enum DispatchState {
        ACCEPTED,
        RUNNING,
        TERMINAL
    }

    public record DispatchRecord(
            int formatVersion,
            String dispatchId,
            String attemptId,
            String taskInstanceId,
            long leaseVersion,
            String workerId,
            String workerEpoch,
            DispatchState state,
            Instant acceptedAt,
            Instant startedAt,
            Instant terminalAt,
            TaskResultReport terminalReport,
            String terminalChecksum,
            int terminalDeliveryAttempts,
            Instant nextTerminalDeliveryAt,
            Instant acknowledgedAt) {
    }
}
