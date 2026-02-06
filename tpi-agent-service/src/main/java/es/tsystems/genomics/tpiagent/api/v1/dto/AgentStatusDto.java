package es.tsystems.genomics.tpiagent.api.v1.dto;

import java.time.Instant;
import java.util.Map;

public class AgentStatusDto {
    private Instant now;

    private String agentId;
    private String client;
    private String env;
    // sourceName eliminado - ahora es dinámico por cada source

    private DirectoriesDto directories;

    private String storageBackendId;

    private String eventsTopic;
    private String stateTopic;

    private long scanIntervalMs;
    private long fileStabilityWindowMs;
    private boolean moveDirectoryDatasets;

    private UploadsSummaryDto uploads;

    public Instant getNow() {
        return now;
    }

    public void setNow(Instant now) {
        this.now = now;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }


    public DirectoriesDto getDirectories() {
        return directories;
    }

    public void setDirectories(DirectoriesDto directories) {
        this.directories = directories;
    }

    public String getStorageBackendId() {
        return storageBackendId;
    }

    public void setStorageBackendId(String storageBackendId) {
        this.storageBackendId = storageBackendId;
    }

    public String getEventsTopic() {
        return eventsTopic;
    }

    public void setEventsTopic(String eventsTopic) {
        this.eventsTopic = eventsTopic;
    }

    public String getStateTopic() {
        return stateTopic;
    }

    public void setStateTopic(String stateTopic) {
        this.stateTopic = stateTopic;
    }

    public long getScanIntervalMs() {
        return scanIntervalMs;
    }

    public void setScanIntervalMs(long scanIntervalMs) {
        this.scanIntervalMs = scanIntervalMs;
    }

    public long getFileStabilityWindowMs() {
        return fileStabilityWindowMs;
    }

    public void setFileStabilityWindowMs(long fileStabilityWindowMs) {
        this.fileStabilityWindowMs = fileStabilityWindowMs;
    }

    public boolean isMoveDirectoryDatasets() {
        return moveDirectoryDatasets;
    }

    public void setMoveDirectoryDatasets(boolean moveDirectoryDatasets) {
        this.moveDirectoryDatasets = moveDirectoryDatasets;
    }

    public UploadsSummaryDto getUploads() {
        return uploads;
    }

    public void setUploads(UploadsSummaryDto uploads) {
        this.uploads = uploads;
    }

    public static class DirectoriesDto {
        private String inbox;
        // source, completed, failed eliminados - ahora son dinámicos por cada source

        public String getInbox() {
            return inbox;
        }

        public void setInbox(String inbox) {
            this.inbox = inbox;
        }
    }
}

