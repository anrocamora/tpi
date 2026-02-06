package es.tsystems.genomics.tpiagent.upload.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Upload {
    private String id;
    private String filePath;
    private long sizeBytes;

    /**
     * Identificador de run (nombre de la carpeta de primer nivel), cuando el upload es por carpeta.
     */
    private String runId;

    /**
     * Nombre del source (carpeta de primer nivel en AGENT_INBOX_DIR), p.ej. MiSeq, Nasertic, NextSeq.
     */
    private String sourceName;

    /**
     * Total de bytes del run (suma de ficheros, excluyendo RunCompletionStatus.xml).
     */
    private long bytesTotal;

    /**
     * Bytes subidos acumulados a nivel run.
     */
    private long bytesUploaded;

    /**
     * Estado por fichero dentro del run.
     */
    private List<UploadItem> items = new ArrayList<>();

    private UploadStatus status;
    private String storageBackend;
    private String s3Bucket;
    private String s3Key;
    private String s3UploadId;
    private int partsTotal;

    /**
     * Modelo de carpeta (catálogo) asociado al upload por run.
     *
     * Nota: se declara aquí para controlar el orden de serialización y que salga antes de partsCompleted.
     */
    private Folder folder;

    private int partsCompleted;
    private List<UploadPart> parts = new ArrayList<>();
    private Map<String, String> metadata;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;
    private String errorCode;
    private String errorMessage;

    public Upload() {
    }

    public Upload(String id) {
        this.id = id;
    }

    // getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public long getSizeBytes() { return sizeBytes; }
    public void setSizeBytes(long sizeBytes) { this.sizeBytes = sizeBytes; }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public long getBytesTotal() { return bytesTotal; }
    public void setBytesTotal(long bytesTotal) { this.bytesTotal = bytesTotal; }

    public long getBytesUploaded() { return bytesUploaded; }
    public void setBytesUploaded(long bytesUploaded) { this.bytesUploaded = bytesUploaded; }

    public UploadStatus getStatus() { return status; }
    public void setStatus(UploadStatus status) { this.status = status; }
    public String getStorageBackend() { return storageBackend; }
    public void setStorageBackend(String storageBackend) { this.storageBackend = storageBackend; }
    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }
    public String getS3Key() { return s3Key; }
    public void setS3Key(String s3Key) { this.s3Key = s3Key; }
    public String getS3UploadId() { return s3UploadId; }
    public void setS3UploadId(String s3UploadId) { this.s3UploadId = s3UploadId; }
    public int getPartsTotal() { return partsTotal; }
    public void setPartsTotal(int partsTotal) { this.partsTotal = partsTotal; }

    public Folder getFolder() { return folder; }
    public void setFolder(Folder folder) { this.folder = folder; }

    public int getPartsCompleted() { return partsCompleted; }
    public void setPartsCompleted(int partsCompleted) { this.partsCompleted = partsCompleted; }
    public List<UploadPart> getParts() { return parts; }
    public void setParts(List<UploadPart> parts) { this.parts = parts; }
    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public List<UploadItem> getItems() { return items; }
    public void setItems(List<UploadItem> items) { this.items = items; }
}
