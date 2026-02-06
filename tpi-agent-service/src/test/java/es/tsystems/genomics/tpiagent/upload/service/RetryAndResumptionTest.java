package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.config.S3ClientFactory;
import es.tsystems.genomics.tpiagent.config.StorageConfigurationProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;


import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class RetryAndResumptionTest {

    @Mock
    private StorageConfigurationProperties storageConfigurationProperties;

    @Mock
    private S3ClientFactory s3ClientFactory;

    @Mock
    private KafkaTemplate<String, UploadEvent> kafkaTemplate;

    @Mock
    private UploadStateStore stateStore;

    @Mock
    private SourceLoggerFactory sourceLoggerFactory;


    private AgentUploadProperties agentUploadProperties;
    private UploadService uploadService;

    @BeforeEach
    void setUp() {
        agentUploadProperties = new AgentUploadProperties();
        agentUploadProperties.setAgentId("test-agent");
        agentUploadProperties.setInboxDirectory("/tmp/test");
        agentUploadProperties.setStorageBackendId("DEFAULT");
        agentUploadProperties.setMaxRetries(3);
        agentUploadProperties.setRetryBackoffMs(1000);
        agentUploadProperties.setMaxRetryBackoffMs(30000);
        agentUploadProperties.setResumptionMaxAgeHours(24);

        uploadService = new UploadService(
                storageConfigurationProperties,
                agentUploadProperties,
                s3ClientFactory,
                kafkaTemplate,
                stateStore,
                sourceLoggerFactory
        );
    }

    @Test
    void testRetryPropertiesConfiguration() {
        assertEquals(3, agentUploadProperties.getMaxRetries());
        assertEquals(1000, agentUploadProperties.getRetryBackoffMs());
        assertEquals(30000, agentUploadProperties.getMaxRetryBackoffMs());
        assertEquals(24, agentUploadProperties.getResumptionMaxAgeHours());
    }

    @Test
    void testStateTopicGeneration() {
        String expectedStateTopic = "tpi.uploads.test-agent.state.v1";
        assertEquals(expectedStateTopic, agentUploadProperties.getEffectiveStateTopic());
    }

    // Test removed: abandonUpload() method was deprecated and removed.
    // The current multi-source run upload flow doesn't use upload abandonment.
    // Uploads are either completed, failed, or aborted if the source file/directory no longer exists.

    @Test
    void testAbortUploadForMissingFileSetsCorrectStatus() {
        Upload upload = new Upload();
        upload.setId("test-upload-2");
        upload.setFilePath("/tmp/test/missing-file.dat");
        upload.setStatus(UploadStatus.IN_PROGRESS);
        upload.setStorageBackend("DEFAULT");

        uploadService.abortUploadForMissingFile(upload);

        assertEquals(UploadStatus.ABORTED, upload.getStatus());
        assertEquals("FILE_NOT_FOUND", upload.getErrorCode());
        assertNotNull(upload.getErrorMessage());
        assertTrue(upload.getErrorMessage().contains("no longer exists"));
    }

    @Test
    void testUploadStatusEnumContainsAbandoned() {
        UploadStatus[] statuses = UploadStatus.values();
        boolean hasAbandoned = false;
        for (UploadStatus status : statuses) {
            if (status == UploadStatus.ABANDONED) {
                hasAbandoned = true;
                break;
            }
        }
        assertTrue(hasAbandoned, "UploadStatus should contain ABANDONED");
    }

    @Test
    void testUploadEventTypeContainsNewEvents() {
        UploadEventType[] types = UploadEventType.values();
        boolean hasAbandoned = false;
        boolean hasPartRetryFailed = false;

        for (UploadEventType type : types) {
            if (type == UploadEventType.UPLOAD_ABANDONED) {
                hasAbandoned = true;
            }
            if (type == UploadEventType.PART_RETRY_FAILED) {
                hasPartRetryFailed = true;
            }
        }

        assertTrue(hasAbandoned, "UploadEventType should contain UPLOAD_ABANDONED");
        assertTrue(hasPartRetryFailed, "UploadEventType should contain PART_RETRY_FAILED");
    }

    @Test
    void testUploadEventHasS3UploadIdField() {
        UploadEvent event = new UploadEvent();
        event.setS3UploadId("test-s3-upload-id");

        assertEquals("test-s3-upload-id", event.getS3UploadId());
    }
}



