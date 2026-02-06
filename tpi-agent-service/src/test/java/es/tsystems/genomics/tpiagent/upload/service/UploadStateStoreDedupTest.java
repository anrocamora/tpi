package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Folder;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStateSnapshot;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class UploadStateStoreDedupTest {

    @Test
    void shouldDeduplicatePartsByUriItemAndPartNumber() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, UploadStateSnapshot> kafkaTemplate = (KafkaTemplate<String, UploadStateSnapshot>) Mockito.mock(KafkaTemplate.class);
        AgentUploadProperties props = Mockito.mock(AgentUploadProperties.class);
        Mockito.when(props.getEffectiveStateTopic()).thenReturn("dummy-state");

        UploadStateStore store = new UploadStateStore(kafkaTemplate, props);

        UploadEvent e1 = new UploadEvent();
        e1.setEventType(UploadEventType.PART_UPLOADED);
        e1.setUploadId("u1");
        e1.setFilePath("C:/run");
        e1.setRunId("R1");
        e1.setItemRelativePath("Data/file1.bin");
        e1.setS3Key("agent/x/R1/Data/file1.bin");
        e1.setPartNumber(1);
        e1.setPartEtag("etag-a");
        e1.setOccurredAt(Instant.now());

        UploadEvent e2 = new UploadEvent();
        e2.setEventType(UploadEventType.PART_UPLOADED);
        e2.setUploadId("u1");
        e2.setFilePath("C:/run");
        e2.setRunId("R1");
        e2.setItemRelativePath("Data/file1.bin");
        e2.setS3Key("agent/x/R1/Data/file1.bin");
        e2.setPartNumber(1);
        e2.setPartEtag("etag-a");
        e2.setOccurredAt(Instant.now());

        store.applyEvent(e1);
        store.applyEvent(e2);

        Upload u = store.getAllUploads().get("u1");
        assertNotNull(u);
        assertEquals(1, u.getParts().size(), "no debe duplicar partes");
        assertEquals(1, u.getParts().get(0).getPartNumber());
        assertEquals("Data/file1.bin", u.getParts().get(0).getItemRelativePath());
        assertEquals("agent/x/R1/Data/file1.bin", u.getParts().get(0).getUri());
    }

    @Test
    void shouldUpdateFolderWhenEventIncludesIt() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, UploadStateSnapshot> kafkaTemplate = (KafkaTemplate<String, UploadStateSnapshot>) Mockito.mock(KafkaTemplate.class);
        AgentUploadProperties props = Mockito.mock(AgentUploadProperties.class);
        Mockito.when(props.getEffectiveStateTopic()).thenReturn("dummy-state");

        UploadStateStore store = new UploadStateStore(kafkaTemplate, props);

        Folder folder = new Folder();
        folder.setName("run");

        UploadEvent started = new UploadEvent();
        started.setEventType(UploadEventType.UPLOAD_STARTED);
        started.setUploadId("u2");
        started.setRunId("R2");
        started.setFilePath("C:/run/R2");
        started.setFolder(folder);
        started.setOccurredAt(Instant.now());

        store.applyEvent(started);

        Upload u = store.getAllUploads().get("u2");
        assertNotNull(u);
        assertNotNull(u.getFolder());
        assertEquals("run", u.getFolder().getName());
    }
}

