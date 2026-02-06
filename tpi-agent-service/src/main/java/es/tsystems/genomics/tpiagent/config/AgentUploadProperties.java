package es.tsystems.genomics.tpiagent.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "agent.upload")
public class AgentUploadProperties {
    @NotBlank
    private String agentId = "tpi-agent-service-snso-001-dev";

    @NotBlank
    private String client = "snso";

    @NotBlank
    private String env = "dev";

    @NotBlank
    private String storageBackendId;

    @Min(5)
    private long partSizeMiB = 64;

    @Min(1000)
    private long scanIntervalMs = 30000;

    private String eventsTopic;

    private String stateTopic;

    @Min(1)
    private int maxRetries = 3;

    @Min(100)
    private long retryBackoffMs = 1000;

    @Min(1000)
    private long maxRetryBackoffMs = 30000;

    @Min(1)
    private int resumptionMaxAgeHours = 24;

    @Min(5)
    private int progressReportIntervalSeconds = 30;

    @Min(1)
    private double progressMinPercentageChange = 10.0;

    /**
     * Número de subidas de archivos concurrentes en modo run.
     * Mayor concurrencia = más rápido, pero consume más memoria y ancho de banda.
     * Valor recomendado: 10-20 para conexiones de alta velocidad.
     */
    @Min(1)
    private int concurrentUploads = 10;

    /**
     * Directorio base donde llegan los ficheros (raíz a escanear).
     * Las subcarpetas de primer nivel representan sources (p.ej. MiSeq, Nasertic, NextSeq).
     * Dentro de cada source se creará la estructura: {sourceName}/{agentId}/{source|completed|failed|logs}.
     */
    @NotBlank
    private String inboxDirectory;

    /**
     * Ventana mínima (ms) durante la cual el fichero debe mantener tamaño y mtime para considerarse completo.
     */
    @Min(1000)
    private long fileStabilityWindowMs = 60000;

    /**
     * Si es true, el scheduler tratará los directorios de primer nivel del inbox como "datasets" y
     * los moverá completos a source cuando estén estables (modo PRE share global).
     */
    private boolean moveDirectoryDatasets = false;

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

    public String getStorageBackendId() {
        return storageBackendId;
    }

    public void setStorageBackendId(String storageBackendId) {
        this.storageBackendId = storageBackendId;
    }

    public long getPartSizeMiB() {
        return partSizeMiB;
    }

    public void setPartSizeMiB(long partSizeMiB) {
        this.partSizeMiB = partSizeMiB;
    }

    public long getScanIntervalMs() {
        return scanIntervalMs;
    }

    public void setScanIntervalMs(long scanIntervalMs) {
        this.scanIntervalMs = scanIntervalMs;
    }

    public String getEventsTopic() {
        return eventsTopic;
    }

    public void setEventsTopic(String eventsTopic) {
        this.eventsTopic = eventsTopic;
    }

    public String getEffectiveEventsTopic() {
        if (eventsTopic == null || eventsTopic.isBlank()) {
            return "tpi.uploads." + agentId + ".events.v1";
        }
        return eventsTopic;
    }

    public String getStateTopic() {
        return stateTopic;
    }

    public void setStateTopic(String stateTopic) {
        this.stateTopic = stateTopic;
    }

    public String getEffectiveStateTopic() {
        if (stateTopic == null || stateTopic.isBlank()) {
            return "tpi.uploads." + agentId + ".state.v1";
        }
        return stateTopic;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public long getMaxRetryBackoffMs() {
        return maxRetryBackoffMs;
    }

    public void setMaxRetryBackoffMs(long maxRetryBackoffMs) {
        this.maxRetryBackoffMs = maxRetryBackoffMs;
    }

    public int getResumptionMaxAgeHours() {
        return resumptionMaxAgeHours;
    }

    public void setResumptionMaxAgeHours(int resumptionMaxAgeHours) {
        this.resumptionMaxAgeHours = resumptionMaxAgeHours;
    }

    public int getProgressReportIntervalSeconds() {
        return progressReportIntervalSeconds;
    }

    public void setProgressReportIntervalSeconds(int progressReportIntervalSeconds) {
        this.progressReportIntervalSeconds = progressReportIntervalSeconds;
    }

    public double getProgressMinPercentageChange() {
        return progressMinPercentageChange;
    }

    public void setProgressMinPercentageChange(double progressMinPercentageChange) {
        this.progressMinPercentageChange = progressMinPercentageChange;
    }

    public int getConcurrentUploads() {
        return concurrentUploads;
    }

    public void setConcurrentUploads(int concurrentUploads) {
        this.concurrentUploads = concurrentUploads;
    }

    public String getInboxDirectory() {
        return inboxDirectory;
    }

    public void setInboxDirectory(String inboxDirectory) {
        this.inboxDirectory = inboxDirectory;
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

    /**
     * Calcula el directorio source para un sourceName específico.
     * @param sourceName Nombre del source (p.ej. MiSeq, Nasertic)
     * @return Ruta del directorio source: {inboxDirectory}/{sourceName}/{agentId}/source
     */
    public String getSourceDirectoryFor(String sourceName) {
        return inboxDirectory + "/" + sourceName + "/" + agentId + "/source";
    }

    /**
     * Calcula el directorio completed para un sourceName específico.
     * @param sourceName Nombre del source (p.ej. MiSeq, Nasertic)
     * @return Ruta del directorio completed: {inboxDirectory}/{sourceName}/{agentId}/completed
     */
    public String getCompletedDirectoryFor(String sourceName) {
        return inboxDirectory + "/" + sourceName + "/" + agentId + "/completed";
    }

    /**
     * Calcula el directorio failed para un sourceName específico.
     * @param sourceName Nombre del source (p.ej. MiSeq, Nasertic)
     * @return Ruta del directorio failed: {inboxDirectory}/{sourceName}/{agentId}/failed
     */
    public String getFailedDirectoryFor(String sourceName) {
        return inboxDirectory + "/" + sourceName + "/" + agentId + "/failed";
    }

    /**
     * Calcula el directorio de logs para un sourceName específico.
     * @param sourceName Nombre del source (p.ej. MiSeq, Nasertic)
     * @return Ruta del directorio logs: {inboxDirectory}/{sourceName}/{agentId}/logs
     */
    public String getLogsDirectoryFor(String sourceName) {
        return inboxDirectory + "/" + sourceName + "/" + agentId + "/logs";
    }
}
