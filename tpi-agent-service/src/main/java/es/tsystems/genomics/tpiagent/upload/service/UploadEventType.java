package es.tsystems.genomics.tpiagent.upload.service;

public enum UploadEventType {
    UPLOAD_STARTED,
    PART_UPLOADED,
    UPLOAD_PROGRESS,
    UPLOAD_COMPLETED,
    UPLOAD_FAILED,
    UPLOAD_ABORTED,
    UPLOAD_ABANDONED,
    PART_RETRY_FAILED
}


