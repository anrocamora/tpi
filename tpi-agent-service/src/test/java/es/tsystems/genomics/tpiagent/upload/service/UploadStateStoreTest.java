package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStateSnapshot;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UploadStateStoreTest {

    private UploadStateStore stateStore;
    private KafkaTemplate<String, UploadStateSnapshot> kafkaTemplate;
    private AgentUploadProperties properties;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        properties = new AgentUploadProperties();
        properties.setAgentId("test-agent");
        stateStore = new UploadStateStore(kafkaTemplate, properties);
    }

    @Test
    void testApplyUploadStartedEvent() {
        UploadEvent event = new UploadEvent();
        event.setEventType(UploadEventType.UPLOAD_STARTED);
        event.setUploadId("upload-1");
        event.setAgentId("agent-1");
        event.setFilePath("/path/to/file.txt");
        event.setSizeBytes(1024L);
        event.setStorageBackend("backend-1");
        event.setS3Bucket("bucket");
        event.setS3Key("key");
        event.setPartsTotal(10);
        event.setOccurredAt(Instant.now());

        stateStore.applyEvent(event);

        Map<String, Upload> uploads = stateStore.getAllUploads();
        assertEquals(1, uploads.size());
        assertTrue(uploads.containsKey("upload-1"));

        Upload upload = uploads.get("upload-1");
        assertEquals("upload-1", upload.getId());
        assertEquals("/path/to/file.txt", upload.getFilePath());
        assertEquals(1024L, upload.getSizeBytes());
        assertEquals(UploadStatus.STARTED, upload.getStatus());
        assertEquals(10, upload.getPartsTotal());
        assertEquals(0, upload.getPartsCompleted());
    }

    @Test
    void testApplyPartUploadedEvent() {
        // Primero iniciamos una carga
        UploadEvent startEvent = new UploadEvent();
        startEvent.setEventType(UploadEventType.UPLOAD_STARTED);
        startEvent.setUploadId("upload-1");
        startEvent.setAgentId("agent-1");
        startEvent.setFilePath("/path/to/file.txt");
        startEvent.setSizeBytes(1024L);
        startEvent.setStorageBackend("backend-1");
        startEvent.setS3Bucket("bucket");
        startEvent.setS3Key("key");
        startEvent.setPartsTotal(5);
        startEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(startEvent);

        // Ahora aplicamos evento de parte subida
        UploadEvent partEvent = new UploadEvent();
        partEvent.setEventType(UploadEventType.PART_UPLOADED);
        partEvent.setUploadId("upload-1");
        partEvent.setPartNumber(1);
        partEvent.setPartEtag("etag-1");
        partEvent.setPartsCompleted(1);
        partEvent.setOccurredAt(Instant.now());

        stateStore.applyEvent(partEvent);

        Upload upload = stateStore.getAllUploads().get("upload-1");
        assertEquals(1, upload.getPartsCompleted());
        assertEquals(5, upload.getPartsTotal());
        assertEquals(1, upload.getParts().size());
        assertEquals("etag-1", upload.getParts().get(0).getEtag());
    }

    @Test
    void testApplyUploadCompletedEvent() {
        // Iniciar carga
        UploadEvent startEvent = new UploadEvent();
        startEvent.setEventType(UploadEventType.UPLOAD_STARTED);
        startEvent.setUploadId("upload-1");
        startEvent.setAgentId("agent-1");
        startEvent.setFilePath("/path/to/file.txt");
        startEvent.setSizeBytes(1024L);
        startEvent.setStorageBackend("backend-1");
        startEvent.setS3Bucket("bucket");
        startEvent.setS3Key("key");
        startEvent.setPartsTotal(1);
        startEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(startEvent);

        // Completar carga
        Instant completedAt = Instant.now();
        UploadEvent completedEvent = new UploadEvent();
        completedEvent.setEventType(UploadEventType.UPLOAD_COMPLETED);
        completedEvent.setUploadId("upload-1");
        completedEvent.setOccurredAt(completedAt);

        stateStore.applyEvent(completedEvent);

        Upload upload = stateStore.getAllUploads().get("upload-1");
        assertEquals(UploadStatus.COMPLETED, upload.getStatus());
        assertNotNull(upload.getCompletedAt());
    }

    @Test
    void testApplyUploadFailedEvent() {
        // Iniciar carga
        UploadEvent startEvent = new UploadEvent();
        startEvent.setEventType(UploadEventType.UPLOAD_STARTED);
        startEvent.setUploadId("upload-1");
        startEvent.setAgentId("agent-1");
        startEvent.setFilePath("/path/to/file.txt");
        startEvent.setSizeBytes(1024L);
        startEvent.setStorageBackend("backend-1");
        startEvent.setS3Bucket("bucket");
        startEvent.setS3Key("key");
        startEvent.setPartsTotal(1);
        startEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(startEvent);

        // Fallar carga
        UploadEvent failedEvent = new UploadEvent();
        failedEvent.setEventType(UploadEventType.UPLOAD_FAILED);
        failedEvent.setUploadId("upload-1");
        failedEvent.setErrorCode("S3_ERROR");
        failedEvent.setErrorMessage("Connection timeout");
        failedEvent.setOccurredAt(Instant.now());

        stateStore.applyEvent(failedEvent);

        Upload upload = stateStore.getAllUploads().get("upload-1");
        assertEquals(UploadStatus.FAILED, upload.getStatus());
        assertEquals("S3_ERROR", upload.getErrorCode());
        assertEquals("Connection timeout", upload.getErrorMessage());
    }

    @Test
    void testHasActiveUploadForPath() {
        UploadEvent event = new UploadEvent();
        event.setEventType(UploadEventType.UPLOAD_STARTED);
        event.setUploadId("upload-1");
        event.setAgentId("agent-1");
        event.setFilePath("/path/to/file.txt");
        event.setSizeBytes(1024L);
        event.setStorageBackend("backend-1");
        event.setS3Bucket("bucket");
        event.setS3Key("key");
        event.setPartsTotal(1);
        event.setOccurredAt(Instant.now());

        stateStore.applyEvent(event);

        assertTrue(stateStore.hasActiveUploadForPath("/path/to/file.txt"));
        assertFalse(stateStore.hasActiveUploadForPath("/path/to/other.txt"));
    }

    @Test
    void testHasActiveUploadForPathReturnsFalseWhenCompleted() {
        // Iniciar carga
        UploadEvent startEvent = new UploadEvent();
        startEvent.setEventType(UploadEventType.UPLOAD_STARTED);
        startEvent.setUploadId("upload-1");
        startEvent.setAgentId("agent-1");
        startEvent.setFilePath("/path/to/file.txt");
        startEvent.setSizeBytes(1024L);
        startEvent.setStorageBackend("backend-1");
        startEvent.setS3Bucket("bucket");
        startEvent.setS3Key("key");
        startEvent.setPartsTotal(1);
        startEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(startEvent);

        assertTrue(stateStore.hasActiveUploadForPath("/path/to/file.txt"));

        // Completar carga
        UploadEvent completedEvent = new UploadEvent();
        completedEvent.setEventType(UploadEventType.UPLOAD_COMPLETED);
        completedEvent.setUploadId("upload-1");
        completedEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(completedEvent);

        assertFalse(stateStore.hasActiveUploadForPath("/path/to/file.txt"));
    }

    @Test
    void testGetAllUploadsReturnsUnmodifiableMap() {
        UploadEvent event = new UploadEvent();
        event.setEventType(UploadEventType.UPLOAD_STARTED);
        event.setUploadId("upload-1");
        event.setAgentId("agent-1");
        event.setFilePath("/path/to/file.txt");
        event.setSizeBytes(1024L);
        event.setStorageBackend("backend-1");
        event.setS3Bucket("bucket");
        event.setS3Key("key");
        event.setPartsTotal(1);
        event.setOccurredAt(Instant.now());

        stateStore.applyEvent(event);

        Map<String, Upload> uploads = stateStore.getAllUploads();
        assertThrows(UnsupportedOperationException.class, () -> uploads.put("test", new Upload()));
    }

    @Test
    void testApplyUploadCompletedEventForRunKeepsFileCounters() {
        // Simulamos un run con 3 ficheros totales y 2 completados.
        UploadEvent startEvent = new UploadEvent();
        startEvent.setEventType(UploadEventType.UPLOAD_STARTED);
        startEvent.setUploadId("upload-run-1");
        startEvent.setAgentId("agent-1");
        startEvent.setRunId("run-123");
        startEvent.setFilePath("/path/to/run");
        startEvent.setPartsTotal(3);
        startEvent.setPartsCompleted(2);
        startEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(startEvent);

        // Añadimos un PART_UPLOADED para simular multipart de un item: no debe pisar contadores de run.
        UploadEvent partEvent = new UploadEvent();
        partEvent.setEventType(UploadEventType.PART_UPLOADED);
        partEvent.setUploadId("upload-run-1");
        partEvent.setRunId("run-123");
        partEvent.setPartNumber(1);
        partEvent.setPartEtag("etag-1");
        partEvent.setS3Key("s3://bucket/key-1");
        partEvent.setItemRelativePath("file1.bin");
        partEvent.setOccurredAt(Instant.now());
        stateStore.applyEvent(partEvent);

        // Completar run
        Instant completedAt = Instant.now();
        UploadEvent completedEvent = new UploadEvent();
        completedEvent.setEventType(UploadEventType.UPLOAD_COMPLETED);
        completedEvent.setUploadId("upload-run-1");
        completedEvent.setRunId("run-123");
        completedEvent.setPartsTotal(3);
        completedEvent.setPartsCompleted(2);
        completedEvent.setOccurredAt(completedAt);
        stateStore.applyEvent(completedEvent);

        Upload upload = stateStore.getAllUploads().get("upload-run-1");
        assertEquals(UploadStatus.COMPLETED, upload.getStatus());
        assertEquals(3, upload.getPartsTotal());
        assertEquals(2, upload.getPartsCompleted());
    }
}
