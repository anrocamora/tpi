package es.tsystems.genomics.tpiagent.upload.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Lightweight DTO for publishing upload state snapshots to Kafka.
 * Contains only essential information for monitoring, avoiding large nested structures.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UploadStateSnapshot {
    private String id;
    private String filePath;
    private long sizeBytes;

    // Run information
    private String runId;
    private String sourceName;
    private long bytesTotal;
    private long bytesUploaded;

    // Progress tracking
    private int itemsTotal;
    private int itemsCompleted;
    private int itemsFailed;

    // Status
    private UploadStatus status;
    private String storageBackend;
    private String s3Bucket;
    private String s3Key;

    // Progress counters (for single file uploads or run-level tracking)
    private int partsTotal;
    private int partsCompleted;

    // Metadata
    private Map<String, String> metadata;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;
    private String errorCode;
    private String errorMessage;

    // Catalog reference (lightweight)
    private String folderName;
    private String folderUrl;

    public UploadStateSnapshot() {
    }

    /**
     * Creates a lightweight snapshot from a full Upload object.
     */
    public static UploadStateSnapshot fromUpload(Upload upload) {
        UploadStateSnapshot snapshot = new UploadStateSnapshot();
        snapshot.setId(upload.getId());
        snapshot.setFilePath(upload.getFilePath());
        snapshot.setSizeBytes(upload.getSizeBytes());

        snapshot.setRunId(upload.getRunId());
        snapshot.setSourceName(upload.getSourceName());
        snapshot.setBytesTotal(upload.getBytesTotal());
        snapshot.setBytesUploaded(upload.getBytesUploaded());

        // Calculate items summary
        if (upload.getItems() != null && !upload.getItems().isEmpty()) {
            snapshot.setItemsTotal(upload.getItems().size());
            snapshot.setItemsCompleted((int) upload.getItems().stream()
                    .filter(item -> item.getStatus() == UploadStatus.COMPLETED)
                    .count());
            snapshot.setItemsFailed((int) upload.getItems().stream()
                    .filter(item -> item.getStatus() == UploadStatus.FAILED)
                    .count());
        }

        snapshot.setStatus(upload.getStatus());
        snapshot.setStorageBackend(upload.getStorageBackend());
        snapshot.setS3Bucket(upload.getS3Bucket());
        snapshot.setS3Key(upload.getS3Key());

        snapshot.setPartsTotal(upload.getPartsTotal());
        snapshot.setPartsCompleted(upload.getPartsCompleted());

        snapshot.setMetadata(upload.getMetadata());
        snapshot.setCreatedAt(upload.getCreatedAt());
        snapshot.setUpdatedAt(upload.getUpdatedAt());
        snapshot.setCompletedAt(upload.getCompletedAt());
        snapshot.setErrorCode(upload.getErrorCode());
        snapshot.setErrorMessage(upload.getErrorMessage());

        // Only include folder name and URL, not the entire recursive structure
        if (upload.getFolder() != null) {
            snapshot.setFolderName(upload.getFolder().getName());
            snapshot.setFolderUrl(upload.getFolder().getUrl());
        }

        return snapshot;
    }

    /**
     * Converts this snapshot back to a basic Upload object (without items, parts, or folder details).
     * This is used for state recovery where we only need the essential upload information.
     */
    public Upload toUpload() {
        Upload upload = new Upload(this.id);
        upload.setFilePath(this.filePath);
        upload.setSizeBytes(this.sizeBytes);

        upload.setRunId(this.runId);
        upload.setSourceName(this.sourceName);
        upload.setBytesTotal(this.bytesTotal);
        upload.setBytesUploaded(this.bytesUploaded);

        upload.setStatus(this.status);
        upload.setStorageBackend(this.storageBackend);
        upload.setS3Bucket(this.s3Bucket);
        upload.setS3Key(this.s3Key);

        upload.setPartsTotal(this.partsTotal);
        upload.setPartsCompleted(this.partsCompleted);

        upload.setMetadata(this.metadata);
        upload.setCreatedAt(this.createdAt);
        upload.setUpdatedAt(this.updatedAt);
        upload.setCompletedAt(this.completedAt);
        upload.setErrorCode(this.errorCode);
        upload.setErrorMessage(this.errorMessage);

        // Note: items, parts, and folder structure are not recovered from snapshot
        // These will be rebuilt from events or remain empty for completed uploads

        return upload;
    }

    // Getters and setters
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

    public int getItemsTotal() { return itemsTotal; }
    public void setItemsTotal(int itemsTotal) { this.itemsTotal = itemsTotal; }

    public int getItemsCompleted() { return itemsCompleted; }
    public void setItemsCompleted(int itemsCompleted) { this.itemsCompleted = itemsCompleted; }

    public int getItemsFailed() { return itemsFailed; }
    public void setItemsFailed(int itemsFailed) { this.itemsFailed = itemsFailed; }

    public UploadStatus getStatus() { return status; }
    public void setStatus(UploadStatus status) { this.status = status; }

    public String getStorageBackend() { return storageBackend; }
    public void setStorageBackend(String storageBackend) { this.storageBackend = storageBackend; }

    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }

    public String getS3Key() { return s3Key; }
    public void setS3Key(String s3Key) { this.s3Key = s3Key; }

    public int getPartsTotal() { return partsTotal; }
    public void setPartsTotal(int partsTotal) { this.partsTotal = partsTotal; }

    public int getPartsCompleted() { return partsCompleted; }
    public void setPartsCompleted(int partsCompleted) { this.partsCompleted = partsCompleted; }

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

    public String getFolderName() { return folderName; }
    public void setFolderName(String folderName) { this.folderName = folderName; }

    public String getFolderUrl() { return folderUrl; }
    public void setFolderUrl(String folderUrl) { this.folderUrl = folderUrl; }
}
