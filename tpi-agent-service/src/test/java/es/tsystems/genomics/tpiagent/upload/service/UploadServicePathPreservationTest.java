package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.config.S3ClientFactory;
import es.tsystems.genomics.tpiagent.config.StorageBackendProperties;
import es.tsystems.genomics.tpiagent.config.StorageBackendType;
import es.tsystems.genomics.tpiagent.config.StorageConfigurationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UploadServicePathPreservationTest {

    @TempDir
    Path tempDir;

    private UploadService uploadService;

    private StorageConfigurationProperties storageConfigurationProperties;
    private AgentUploadProperties agentUploadProperties;
    private S3ClientFactory s3ClientFactory;
    private KafkaTemplate<String, UploadEvent> kafkaTemplate;
    private UploadStateStore stateStore;

    private software.amazon.awssdk.services.s3.S3Client s3Client;

    @BeforeEach
    void setUp() {
        agentUploadProperties = new AgentUploadProperties();
        agentUploadProperties.setAgentId("agent-1");
        agentUploadProperties.setStorageBackendId("DEFAULT");
        agentUploadProperties.setPartSizeMiB(64);
        agentUploadProperties.setMaxRetries(1);
        agentUploadProperties.setRetryBackoffMs(10);
        agentUploadProperties.setMaxRetryBackoffMs(10);
        agentUploadProperties.setResumptionMaxAgeHours(24);

        storageConfigurationProperties = mock(StorageConfigurationProperties.class);
        StorageBackendProperties backend = new StorageBackendProperties();
        backend.setId("DEFAULT");
        backend.setType(StorageBackendType.AWS);
        backend.setBucket("bucket");
        backend.setBasePath("agent/");
        backend.setUseSinglePartUpload(true); // evita multipart

        when(storageConfigurationProperties.backendMap()).thenReturn(Map.of("DEFAULT", backend));

        s3ClientFactory = mock(S3ClientFactory.class);
        s3Client = mock(software.amazon.awssdk.services.s3.S3Client.class);
        when(s3ClientFactory.clientFor(anyString())).thenReturn(s3Client);

        // Respuestas S3 simuladas
        when(s3Client.putObject(any(PutObjectRequest.class), any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("etag-1").build());
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().eTag("etag-1").contentLength(4L).build());

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, UploadEvent> kafkaTemplate = (KafkaTemplate<String, UploadEvent>) mock(KafkaTemplate.class);

        stateStore = mock(UploadStateStore.class);
        when(stateStore.hasActiveUploadForPath(anyString())).thenReturn(false);

        SourceLoggerFactory sourceLoggerFactory = mock(SourceLoggerFactory.class);
        when(sourceLoggerFactory.getLoggerForSource(anyString(), anyString())).thenReturn(org.slf4j.LoggerFactory.getLogger("test"));

        uploadService = new UploadService(storageConfigurationProperties, agentUploadProperties, s3ClientFactory, kafkaTemplate, stateStore, sourceLoggerFactory);
    }

    @Test
    void uploadUsesRelativePathInS3Key() throws Exception {
        Path inbox = tempDir.resolve("inbox");
        Files.createDirectories(inbox);

        agentUploadProperties.setInboxDirectory(inbox.toString());

        // Crear estructura: inbox/TestSource/agent-1/source/run01/...
        Path sourceDir = inbox.resolve("TestSource").resolve("agent-1").resolve("source");
        Files.createDirectories(sourceDir);

        Path runDir = sourceDir.resolve("run01");
        Files.createDirectories(runDir);
        Path file = runDir.resolve("reads.fastq.gz");
        Files.writeString(file, "data");
        // Flag requerido (pero ignorado en la subida)
        Files.writeString(runDir.resolve("RunCompletionStatus.xml"), "<ok/>");

        uploadService.uploadRunFolder(runDir, "TestSource");

        ArgumentCaptor<UploadEvent> eventCaptor = ArgumentCaptor.forClass(UploadEvent.class);
        verify(stateStore, atLeastOnce()).applyEvent(eventCaptor.capture());

        List<UploadEvent> events = eventCaptor.getAllValues();
        assertFalse(events.isEmpty());

        // Para run, el prefix debe ser agent/<sourceName>/<agentId>/<runId>/...
        String expectedKey = "agent/TestSource/agent-1/run01/reads.fastq.gz";
        assertTrue(events.stream().anyMatch(e -> expectedKey.equals(e.getS3Key())),
                "Expected at least one event with s3Key=" + expectedKey);

        // El flag no debe subirse (no debe aparecer una key que termine en RunCompletionStatus.xml)
        assertFalse(events.stream().anyMatch(e -> e.getS3Key() != null && e.getS3Key().endsWith("RunCompletionStatus.xml")));
    }
}
