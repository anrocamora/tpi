package es.tsystems.genomics.tpiagent.upload.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-file state inside a folder (run) upload.
 */
public class UploadItem {
    private String relativePath;
    private long sizeBytes;
    private UploadStatus status;

    private String s3Bucket;
    private String s3Key;
    private String s3UploadId;

    private int partsTotal;
    private int partsCompleted;
    private List<UploadPart> parts = new ArrayList<>();

    public UploadItem() {
    }

    public UploadItem(String relativePath) {
        this.relativePath = relativePath;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public UploadStatus getStatus() {
        return status;
    }

    public void setStatus(UploadStatus status) {
        this.status = status;
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

    public String getS3UploadId() {
        return s3UploadId;
    }

    public void setS3UploadId(String s3UploadId) {
        this.s3UploadId = s3UploadId;
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

    public List<UploadPart> getParts() {
        return parts;
    }

    public void setParts(List<UploadPart> parts) {
        this.parts = parts;
    }
}

