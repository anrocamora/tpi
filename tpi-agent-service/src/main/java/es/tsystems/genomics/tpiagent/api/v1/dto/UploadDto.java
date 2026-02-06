package es.tsystems.genomics.tpiagent.api.v1.dto;

import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;

import java.time.Instant;

public class UploadDto {
    private String uploadId;

    private String runId;
    private boolean runIdDerived;

    private UploadStatus status;

    private String sourcePath;

    private String storageBackend;
    private String s3Bucket;
    private String s3Key;

    private long sizeBytes;

    private long bytesTotal;
    private long bytesUploaded;
    private Double progressPct;

    private int partsTotal;
    private int partsCompleted;

    private int itemsCount;
    private int partsCount;

    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;

    private String errorCode;
    private String errorMessage;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public boolean isRunIdDerived() {
        return runIdDerived;
    }

    public void setRunIdDerived(boolean runIdDerived) {
        this.runIdDerived = runIdDerived;
    }

    public UploadStatus getStatus() {
        return status;
    }

    public void setStatus(UploadStatus status) {
        this.status = status;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getStorageBackend() {
        return storageBackend;
    }

    public void setStorageBackend(String storageBackend) {
        this.storageBackend = storageBackend;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3Key() {
        return s3Key;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public long getBytesTotal() {
        return bytesTotal;
    }

    public void setBytesTotal(long bytesTotal) {
        this.bytesTotal = bytesTotal;
    }

    public long getBytesUploaded() {
        return bytesUploaded;
    }

    public void setBytesUploaded(long bytesUploaded) {
        this.bytesUploaded = bytesUploaded;
    }

    public Double getProgressPct() {
        return progressPct;
    }

    public void setProgressPct(Double progressPct) {
        this.progressPct = progressPct;
    }

    public int getPartsTotal() {
        return partsTotal;
    }

    public void setPartsTotal(int partsTotal) {
        this.partsTotal = partsTotal;
    }

    public int getPartsCompleted() {
        return partsCompleted;
    }

    public void setPartsCompleted(int partsCompleted) {
        this.partsCompleted = partsCompleted;
    }

    public int getItemsCount() {
        return itemsCount;
    }

    public void setItemsCount(int itemsCount) {
        this.itemsCount = itemsCount;
    }

    public int getPartsCount() {
        return partsCount;
    }

    public void setPartsCount(int partsCount) {
        this.partsCount = partsCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}

