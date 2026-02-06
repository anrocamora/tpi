package es.tsystems.genomics.tpiagent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AgentUploadPropertiesTest {

    @Test
    void testGettersAndSetters() {
        AgentUploadProperties props = new AgentUploadProperties();

        props.setAgentId("test-agent");
        props.setInboxDirectory("/tmp/data");
        props.setStorageBackendId("backend-1");
        props.setPartSizeMiB(128);
        props.setScanIntervalMs(60000);
        props.setEventsTopic("custom.topic");

        assertEquals("test-agent", props.getAgentId());
        assertEquals("/tmp/data", props.getInboxDirectory());
        assertEquals("backend-1", props.getStorageBackendId());
        assertEquals(128, props.getPartSizeMiB());
        assertEquals(60000, props.getScanIntervalMs());
        assertEquals("custom.topic", props.getEventsTopic());
    }

    @Test
    void testDynamicDirectoriesForSource() {
        AgentUploadProperties props = new AgentUploadProperties();
        props.setAgentId("test-agent");
        props.setInboxDirectory("/tmp/data");

        assertEquals("/tmp/data/MiSeq/test-agent/source", props.getSourceDirectoryFor("MiSeq"));
        assertEquals("/tmp/data/MiSeq/test-agent/completed", props.getCompletedDirectoryFor("MiSeq"));
        assertEquals("/tmp/data/MiSeq/test-agent/failed", props.getFailedDirectoryFor("MiSeq"));
        assertEquals("/tmp/data/MiSeq/test-agent/logs", props.getLogsDirectoryFor("MiSeq"));

        assertEquals("/tmp/data/NextSeq/test-agent/source", props.getSourceDirectoryFor("NextSeq"));
        assertEquals("/tmp/data/NextSeq/test-agent/completed", props.getCompletedDirectoryFor("NextSeq"));
    }

    @Test
    void testEffectiveEventsTopicWithCustomTopic() {
        AgentUploadProperties props = new AgentUploadProperties();
        props.setAgentId("test-agent");
        props.setEventsTopic("custom.topic.v1");

        assertEquals("custom.topic.v1", props.getEffectiveEventsTopic());
    }

    @Test
    void testEffectiveEventsTopicWithNullTopic() {
        AgentUploadProperties props = new AgentUploadProperties();
        props.setAgentId("my-agent");
        props.setEventsTopic(null);

        assertEquals("tpi.uploads.my-agent.events.v1", props.getEffectiveEventsTopic());
    }

    @Test
    void testEffectiveEventsTopicWithBlankTopic() {
        AgentUploadProperties props = new AgentUploadProperties();
        props.setAgentId("agent-123");
        props.setEventsTopic("   ");

        assertEquals("tpi.uploads.agent-123.events.v1", props.getEffectiveEventsTopic());
    }

    @Test
    void testDefaultAgentId() {
        AgentUploadProperties props = new AgentUploadProperties();
        assertEquals("tpi-agent-service-snso-001-dev", props.getAgentId());
    }

    @Test
    void testDefaultPartSizeMiB() {
        AgentUploadProperties props = new AgentUploadProperties();
        assertEquals(64, props.getPartSizeMiB());
    }

    @Test
    void testDefaultScanIntervalMs() {
        AgentUploadProperties props = new AgentUploadProperties();
        assertEquals(30000, props.getScanIntervalMs());
    }
}



