package es.tsystems.genomics.tpiagent.upload.service;

import java.time.Instant;
import java.util.Map;

public class UploadEvent {
    private UploadEventType eventType;
    private String uploadId;
    private String agentId;

    /**
     * Nombre del source (carpeta de primer nivel en AGENT_INBOX_DIR), p.ej. MiSeq, Nasertic, NextSeq.
     */
    private String sourceName;

    /**
     * Identificador de run/carpeta (nombre de la carpeta de primer nivel).
     */
    private String runId;

    /**
     * Path relativo del fichero dentro del run (cuando el evento es por item).
     */
    private String itemRelativePath;

    private String filePath;
    private long sizeBytes;

    /**
     * Total de bytes del run (suma de ficheros, excluyendo RunCompletionStatus.xml).
     */
    private Long bytesTotal;

    /**
     * Bytes subidos acumulados (a nivel run).
     */
    private Long bytesUploaded;

    private String storageBackend;
    private String s3Bucket;
    private String s3Key;
    private String s3UploadId;
    private Instant occurredAt;
    private String errorCode;
    private String errorMessage;
    private Integer partNumber;
    private String partEtag;
    private Integer partsCompleted;
    private Integer partsTotal;
    private Double progressPercentage;
    private Map<String, String> metadata;

    /**
     * Folder/run catalog information associated with this upload.
     */
    private es.tsystems.genomics.tpiagent.upload.model.Folder folder;

    public UploadEvent() {
    }

    // getters and setters
    public UploadEventType getEventType() { return eventType; }
    public void setEventType(UploadEventType eventType) { this.eventType = eventType; }
    public String getUploadId() { return uploadId; }
    public void setUploadId(String uploadId) { this.uploadId = uploadId; }
    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }
    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getItemRelativePath() { return itemRelativePath; }
    public void setItemRelativePath(String itemRelativePath) { this.itemRelativePath = itemRelativePath; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public long getSizeBytes() { return sizeBytes; }
    public void setSizeBytes(long sizeBytes) { this.sizeBytes = sizeBytes; }
    public Long getBytesTotal() { return bytesTotal; }
    public void setBytesTotal(Long bytesTotal) { this.bytesTotal = bytesTotal; }
    public Long getBytesUploaded() { return bytesUploaded; }
    public void setBytesUploaded(Long bytesUploaded) { this.bytesUploaded = bytesUploaded; }
    public String getStorageBackend() { return storageBackend; }
    public void setStorageBackend(String storageBackend) { this.storageBackend = storageBackend; }
    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }
    public String getS3Key() { return s3Key; }
    public void setS3Key(String s3Key) { this.s3Key = s3Key; }
    public String getS3UploadId() { return s3UploadId; }
    public void setS3UploadId(String s3UploadId) { this.s3UploadId = s3UploadId; }
    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Integer getPartNumber() { return partNumber; }
    public void setPartNumber(Integer partNumber) { this.partNumber = partNumber; }
    public String getPartEtag() { return partEtag; }
    public void setPartEtag(String partEtag) { this.partEtag = partEtag; }
    public Integer getPartsCompleted() { return partsCompleted; }
    public void setPartsCompleted(Integer partsCompleted) { this.partsCompleted = partsCompleted; }
    public Integer getPartsTotal() { return partsTotal; }
    public void setPartsTotal(Integer partsTotal) { this.partsTotal = partsTotal; }
    public Double getProgressPercentage() { return progressPercentage; }
    public void setProgressPercentage(Double progressPercentage) { this.progressPercentage = progressPercentage; }
    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }

    public es.tsystems.genomics.tpiagent.upload.model.Folder getFolder() { return folder; }
    public void setFolder(es.tsystems.genomics.tpiagent.upload.model.Folder folder) { this.folder = folder; }
}
