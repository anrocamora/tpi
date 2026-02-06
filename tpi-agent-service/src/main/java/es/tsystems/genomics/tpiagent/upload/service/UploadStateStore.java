package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadPart;
import es.tsystems.genomics.tpiagent.upload.model.UploadStateSnapshot;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class UploadStateStore {

    private static final Logger log = LoggerFactory.getLogger(UploadStateStore.class);

    private final Map<String, Upload> uploads = new ConcurrentHashMap<>();
    private final Map<String, String> uploadIdByRunId = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean recoveryComplete = new AtomicBoolean(false);
    private final KafkaTemplate<String, UploadStateSnapshot> stateSnapshotTemplate;
    private final AgentUploadProperties properties;

    public UploadStateStore(KafkaTemplate<String, UploadStateSnapshot> stateSnapshotTemplate,
                           AgentUploadProperties properties) {
        this.stateSnapshotTemplate = stateSnapshotTemplate;
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        log.info("=================================================");
        log.info("UploadStateStore initialized");
        log.info("State topic: {}", properties.getEffectiveStateTopic());
        log.info("Events topic: {}", properties.getEffectiveEventsTopic());
        log.info("Agent ID: {}", properties.getAgentId());
        log.info("Consumer group (state-recovery): {}-state-recovery", properties.getAgentId());
        log.info("Consumer group (events): {}-state", properties.getAgentId());
        log.info("=================================================");
    }

    @KafkaListener(
            topics = "${agent.upload.events-topic:tpi.uploads.${AGENT_ID:tpi-agent-local}.events.v1}",
            groupId = "${agent.upload.agent-id:${AGENT_ID:tpi-agent-local}}-state")
    public void consume(UploadEvent event) {
        // Durante la recuperación inicial, solo procesamos eventos del state topic
        // Ignoramos eventos nuevos hasta que la recuperación esté completa
        if (!recoveryComplete.get()) {
            log.debug("Ignoring event {} during initial recovery phase", event.getEventType());
            return;
        }
        applyEvent(event);
    }

    @KafkaListener(
            topics = "${agent.upload.state-topic:tpi.uploads.${AGENT_ID:tpi-agent-local}.state.v1}",
            groupId = "${agent.upload.agent-id:${AGENT_ID:tpi-agent-local}}-state-recovery",
            containerFactory = "stateRecoveryListenerFactory",
            autoStartup = "true")
    public void consumeStateSnapshot(UploadStateSnapshot snapshot) {
        log.info("consumeStateSnapshot CALLED with upload: id={}, ready={}", snapshot.getId(), ready.get());

        Upload upload = snapshot.toUpload();

        if (!ready.get()) {
            uploads.put(upload.getId(), upload);
            log.info("Recovered upload state: id={}, filePath={}, status={}, partsCompleted={}/{}, s3Bucket={}",
                upload.getId(), upload.getFilePath(), upload.getStatus(),
                upload.getPartsCompleted(), upload.getPartsTotal(), upload.getS3Bucket());
        } else {
            // Si ya está ready, significa que este mensaje llegó después de la recuperación
            // Lo procesamos normalmente
            uploads.put(upload.getId(), upload);
            log.info("Received state snapshot for upload {} after recovery: status={}, parts={}/{}",
                upload.getId(), upload.getStatus(), upload.getPartsCompleted(), upload.getPartsTotal());
        }
    }

    public void waitUntilReady() {
        if (ready.get()) {
            return;
        }

        // Limpiar estado previo para asegurar que solo usamos lo recuperado de Kafka
        uploads.clear();
        log.info("UploadStateStore is initializing, waiting for state recovery from Kafka...");
        log.info("State topic: {}", properties.getEffectiveStateTopic());
        log.info("Consumer group: {}-state-recovery", properties.getAgentId());

        // Dar tiempo inicial para que el listener de Kafka se conecte y empiece a consumir
        log.info("Waiting 2 seconds for Kafka listener to connect...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during initial wait");
        }

        // Wait for Kafka consumers to catch up with a more generous timeout
        int maxWaitSeconds = 20; // Aumentado de 15 a 20 segundos
        int pollIntervalMs = 500;
        int totalPolls = (maxWaitSeconds * 1000) / pollIntervalMs;

        int previousSize = -1;
        int unchangedPolls = 0;
        int minUnchangedPolls = 8; // Aumentado de 6 a 8 (4 segundos sin cambios)

        for (int i = 0; i < totalPolls; i++) {
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for state recovery");
                break;
            }

            int currentSize = uploads.size();

            if (currentSize == previousSize) {
                unchangedPolls++;
                if (unchangedPolls >= minUnchangedPolls) {
                    log.info("State recovery stabilized at {} upload(s) after {} ms",
                        currentSize, (i + 1) * pollIntervalMs + 2000);
                    break;
                }
            } else {
                unchangedPolls = 0;
                previousSize = currentSize;
                log.info("State recovery in progress: {} upload(s) recovered so far", currentSize);
            }

            // Log cada 5 segundos si no hay cambios
            if (i > 0 && i % 10 == 0 && currentSize == 0) {
                log.warn("Still waiting for messages from state topic... ({}s elapsed, {} uploads so far)",
                    (i * pollIntervalMs + 2000) / 1000, currentSize);
            }
        }

        ready.set(true);
        recoveryComplete.set(true); // Marca la recuperación como completa

        long inProgressCount = uploads.values().stream()
            .filter(u -> u.getStatus() == UploadStatus.IN_PROGRESS)
            .count();

        log.info("UploadStateStore is ready. Recovered {} upload(s) from state topic ({} IN_PROGRESS)",
            uploads.size(), inProgressCount);

        // Log detailed information about recovered uploads
        if (inProgressCount > 0) {
            log.info("Pending uploads to recover:");
            uploads.values().stream()
                .filter(u -> u.getStatus() == UploadStatus.IN_PROGRESS)
                .forEach(u -> log.info("  - {} | {} | {}/{} parts",
                    u.getId(), u.getFilePath(), u.getPartsCompleted(), u.getPartsTotal()));
        } else if (!uploads.isEmpty()) {
            log.info("All {} recovered upload(s) are in terminal state (COMPLETED, FAILED, ABORTED, or ABANDONED)",
                uploads.size());
        } else {
            log.info("No uploads found in state topic after waiting {} seconds", maxWaitSeconds + 2);
        }
    }

    public Upload applyEvent(UploadEvent event) {
        Upload upload = uploads.computeIfAbsent(event.getUploadId(), id -> {
            Upload fresh = new Upload(id);
            fresh.setFilePath(event.getFilePath());
            fresh.setSizeBytes(event.getSizeBytes());
            fresh.setStorageBackend(event.getStorageBackend());
            fresh.setS3Bucket(event.getS3Bucket());
            fresh.setS3Key(event.getS3Key());
            fresh.setMetadata(event.getMetadata());
            fresh.setCreatedAt(event.getOccurredAt());
            // Estado inicial coherente: el primer snapshot se publicará cuando llegue UPLOAD_STARTED.
            fresh.setStatus(UploadStatus.STARTED);
            fresh.setRunId(event.getRunId());
            // folder puede venir ya precargado en el evento (modo run)
            if (event.getFolder() != null) {
                fresh.setFolder(event.getFolder());
            }
            if (event.getRunId() != null && !event.getRunId().isBlank()) {
                uploadIdByRunId.put(event.getRunId(), id);
            }
            return fresh;
        });

        // Mantener runId/index actualizado
        if (event.getRunId() != null && !event.getRunId().isBlank()) {
            upload.setRunId(event.getRunId());
            uploadIdByRunId.put(event.getRunId(), upload.getId());
        }

        // Mantener folder actualizado (solo si viene en el evento; nunca sobrescribir con null)
        if (event.getFolder() != null) {
            upload.setFolder(event.getFolder());
        }

        if (event.getBytesTotal() != null) {
            upload.setBytesTotal(event.getBytesTotal());
        }
        if (event.getBytesUploaded() != null) {
            upload.setBytesUploaded(event.getBytesUploaded());
        }

        // Si es un upload por run/carpeta, algunos productores antiguos pueden mandar sizeBytes=0.
        // En ese caso, usamos bytesTotal como tamaño real del upload.
        if (upload.getSizeBytes() == 0L && upload.getBytesTotal() > 0L) {
            upload.setSizeBytes(upload.getBytesTotal());
        }

        upload.setUpdatedAt(event.getOccurredAt());
        switch (event.getEventType()) {
            case UPLOAD_STARTED -> {
                // En state topic queremos reflejar un estado STARTED visible.
                upload.setStatus(UploadStatus.STARTED);

                // En uploads por fichero viene calculado en el evento.
                // En uploads por run, el productor puede mandar ya contadores de ficheros.
                if (event.getPartsTotal() != null && event.getPartsTotal() > 0) {
                    upload.setPartsTotal(event.getPartsTotal());
                }

                // En STARTED normalmente partsCompleted=0, pero en modo run el productor puede mandar progreso.
                if (event.getPartsCompleted() != null && event.getPartsCompleted() >= 0) {
                    upload.setPartsCompleted(event.getPartsCompleted());
                } else {
                    upload.setPartsCompleted(0);
                }

                upload.setS3UploadId(event.getS3UploadId());
            }
            case PART_UPLOADED -> {
                // Evitar duplicados: puede ocurrir por reintentos/republicaciones.
                // Dedupe por (uri=s3Key, itemRelativePath, partNumber).
                // NOTA: algunos productores/tests no envían s3Key ni itemRelativePath en PART_UPLOADED;
                // en ese caso hacemos fallback al estado ya conocido.
                String uri = event.getS3Key() != null ? event.getS3Key() : upload.getS3Key();
                String itemRelativePath = event.getItemRelativePath() != null ? event.getItemRelativePath() : upload.getFilePath();
                int partNumber = Optional.ofNullable(event.getPartNumber()).orElse(0);
                String etag = event.getPartEtag();

                boolean alreadyPresent = upload.getParts().stream().anyMatch(p ->
                        p.getPartNumber() == partNumber &&
                        java.util.Objects.equals(p.getUri(), uri) &&
                        java.util.Objects.equals(p.getItemRelativePath(), itemRelativePath)
                );

                if (!alreadyPresent) {
                    upload.getParts().add(new UploadPart(partNumber, etag, itemRelativePath, uri));
                } else {
                    log.debug("Ignoring duplicated part uploadId={}, uri={}, item={}, part={} (etag={})",
                            upload.getId(), uri, itemRelativePath, partNumber, etag);
                }

                // El contador consistente se recalcula al final en normalizePartCounters().
                // Aun así, si el producer manda valores, los mantenemos como best-effort.
                upload.setPartsCompleted(Optional.ofNullable(event.getPartsCompleted()).orElse(upload.getPartsCompleted()));
                upload.setPartsTotal(Optional.ofNullable(event.getPartsTotal()).orElse(upload.getPartsTotal()));
            }
            case UPLOAD_PROGRESS -> {
                log.debug("Upload progress update for {} (runId={}, item={}): {}% bytes={}/{}",
                        event.getUploadId(), event.getRunId(), event.getItemRelativePath(), event.getProgressPercentage(),
                        event.getBytesUploaded(), event.getBytesTotal());
            }
            case UPLOAD_COMPLETED -> {
                upload.setStatus(UploadStatus.COMPLETED);
                upload.setCompletedAt(event.getOccurredAt());

                // En modo run, mantener contadores de ficheros si vienen en el evento.
                if (event.getPartsTotal() != null && event.getPartsTotal() > 0) {
                    upload.setPartsTotal(event.getPartsTotal());
                }
                if (event.getPartsCompleted() != null && event.getPartsCompleted() >= 0) {
                    upload.setPartsCompleted(event.getPartsCompleted());
                }
            }
            case UPLOAD_FAILED, UPLOAD_ABORTED, UPLOAD_ABANDONED -> {
                UploadStatus newStatus = switch (event.getEventType()) {
                    case UPLOAD_FAILED -> UploadStatus.FAILED;
                    case UPLOAD_ABORTED -> UploadStatus.ABORTED;
                    case UPLOAD_ABANDONED -> UploadStatus.ABANDONED;
                    default -> upload.getStatus();
                };
                upload.setStatus(newStatus);
                upload.setErrorCode(event.getErrorCode());
                upload.setErrorMessage(event.getErrorMessage());

                // En modo run, mantener contadores si vienen en el evento.
                if (event.getPartsTotal() != null && event.getPartsTotal() > 0) {
                    upload.setPartsTotal(event.getPartsTotal());
                }
                if (event.getPartsCompleted() != null && event.getPartsCompleted() >= 0) {
                    upload.setPartsCompleted(event.getPartsCompleted());
                }
            }
            case PART_RETRY_FAILED -> {
                log.warn("Part {} retry failed for upload {}", event.getPartNumber(), event.getUploadId());
            }
        }

        normalizePartCounters(upload);

        publishStateSnapshot(upload);

        return upload;
    }

    private void normalizePartCounters(Upload upload) {
        // Para uploads por run, partsTotal/partsCompleted representan ficheros del run.
        // Por eso NO debemos recalcularlos a partir de la lista de parts multipart (que pertenece a items).
        if (upload.getRunId() != null && !upload.getRunId().isBlank()) {
            return;
        }

        // Para uploads por run/item, el contador real es el número de (itemRelativePath, partNumber) distintos.
        // Para uploads por fichero normal, partsTotal viene del evento STARTED y partsCompleted viene del último PART_UPLOADED.
        int uniqueParts = (int) upload.getParts().stream()
                .map(p -> {
                    String uri = p.getUri() == null ? "" : p.getUri();
                    String item = p.getItemRelativePath() == null ? "" : p.getItemRelativePath();
                    return uri + "|" + item + "|" + p.getPartNumber();
                })
                .distinct()
                .count();

        if (uniqueParts > 0) {
            // Si ya tenemos parts list, que los contadores reflejen esa realidad.
            upload.setPartsCompleted(uniqueParts);
            if (upload.getPartsTotal() <= 0 || upload.getPartsTotal() < uniqueParts) {
                upload.setPartsTotal(uniqueParts);
            }
        }

        if (upload.getStatus() == UploadStatus.STARTED) {
            // STARTED solo debe forzar 0 mientras no tengamos ninguna parte registrada.
            // En algunos flujos (p.ej. tests/unit o eventos que no cambian status), pueden llegar PART_UPLOADED
            // sin transicionar a IN_PROGRESS; en ese caso debemos reflejar partsCompleted real.
            if (uniqueParts == 0) {
                upload.setPartsCompleted(0);
            }
        }

        if (upload.getStatus() == UploadStatus.COMPLETED) {
            if (upload.getPartsTotal() > 0) {
                upload.setPartsCompleted(upload.getPartsTotal());
            }
        }
    }

    private void publishStateSnapshot(Upload upload) {
        try {
            String stateTopic = properties.getEffectiveStateTopic();
            UploadStateSnapshot snapshot = UploadStateSnapshot.fromUpload(upload);
            stateSnapshotTemplate.send(stateTopic, upload.getId(), snapshot);
            log.debug("Published state snapshot for upload {} to topic {}", upload.getId(), stateTopic);
        } catch (Exception e) {
            log.error("Failed to publish state snapshot for upload {}", upload.getId(), e);
        }
    }

    public boolean hasActiveUploadForPath(String filePath) {
        return uploads.values().stream()
                .anyMatch(u -> filePath.equals(u.getFilePath()) &&
                        (u.getStatus() == UploadStatus.IN_PROGRESS || u.getStatus() == UploadStatus.STARTED));
    }

    public Optional<Upload> findByPath(String filePath) {
        return uploads.values().stream().filter(u -> filePath.equals(u.getFilePath())).findFirst();
    }

    public Optional<Upload> findByRunId(String runId) {
        if (runId == null || runId.isBlank()) {
            return Optional.empty();
        }
        String uploadId = uploadIdByRunId.get(runId);
        if (uploadId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(uploads.get(uploadId));
    }

    public Map<String, Upload> getAllUploads() {
        return Map.copyOf(uploads);
    }
}
