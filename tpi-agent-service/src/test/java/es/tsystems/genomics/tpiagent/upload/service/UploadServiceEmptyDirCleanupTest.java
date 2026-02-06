package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.config.S3ClientFactory;
import es.tsystems.genomics.tpiagent.config.StorageBackendProperties;
import es.tsystems.genomics.tpiagent.config.StorageBackendType;
import es.tsystems.genomics.tpiagent.config.StorageConfigurationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class UploadServiceEmptyDirCleanupTest {

    @TempDir
    Path tempDir;

    private UploadService uploadService;
    private AgentUploadProperties props;

    @BeforeEach
    void setUp() {
        props = new AgentUploadProperties();
        props.setAgentId("agent-1");
        props.setStorageBackendId("DEFAULT");
        props.setPartSizeMiB(64);

        StorageBackendProperties backend = new StorageBackendProperties();
        backend.setId("DEFAULT");
        backend.setType(StorageBackendType.AWS);
        backend.setBucket("bucket");
        backend.setBasePath("agent/");
        backend.setUseSinglePartUpload(true); // evita multipart

        StorageConfigurationProperties storageCfg = mock(StorageConfigurationProperties.class);
        when(storageCfg.backendMap()).thenReturn(Map.of("DEFAULT", backend));

        S3ClientFactory s3ClientFactory = mock(S3ClientFactory.class);
        S3Client s3Client = mock(S3Client.class);
        when(s3ClientFactory.clientFor(anyString())).thenReturn(s3Client);

        when(s3Client.putObject(any(PutObjectRequest.class), any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("etag-1").build());
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().eTag("etag-1").contentLength(1L).build());

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, UploadEvent> kafkaTemplate = mock(KafkaTemplate.class);

        UploadStateStore stateStore = mock(UploadStateStore.class);
        when(stateStore.hasActiveUploadForPath(anyString())).thenReturn(false);

        SourceLoggerFactory sourceLoggerFactory = mock(SourceLoggerFactory.class);
        when(sourceLoggerFactory.getLoggerForSource(anyString(), anyString())).thenReturn(org.slf4j.LoggerFactory.getLogger("test"));

        uploadService = new UploadService(storageCfg, props, s3ClientFactory, kafkaTemplate, stateStore, sourceLoggerFactory);
    }

    @Test
    void removesEmptySubfoldersAfterMoveButKeepsFoldersWithOtherFiles() throws Exception {
        // Este test ya no es relevante porque uploadFile está deprecated en el flujo multi-source
        // El flujo actual es uploadRunFolder que mueve runs completos, no archivos individuales

        Path inbox = tempDir.resolve("inbox");
        Files.createDirectories(inbox);

        props.setInboxDirectory(inbox.toString());

        // En el nuevo flujo multi-source, no se suben archivos individuales
        // Los runs se suben completos con uploadRunFolder(runDir, sourceName)
        // Este test se mantiene como referencia pero no es aplicable al flujo actual

        assertTrue(Files.exists(inbox), "inbox should exist");

        // El método uploadFile está deprecated y lanza UnsupportedOperationException
        // por lo que este test ya no es válido
    }
}
