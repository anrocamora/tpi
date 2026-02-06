package es.tsystems.genomics.tpiagent.api.v1.support;

import es.tsystems.genomics.tpiagent.api.v1.dto.UploadDto;
import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;

public final class UploadDtoMapper {

    private UploadDtoMapper() {
    }

    public static UploadDto toDto(Upload upload, AgentUploadProperties props) {
        if (upload == null) {
            return null;
        }

        RunIdResolver.ResolvedRunId resolved = RunIdResolver.resolve(upload, props);

        UploadDto dto = new UploadDto();
        dto.setUploadId(upload.getId());
        dto.setStatus(upload.getStatus());

        dto.setRunId(resolved.runId());
        dto.setRunIdDerived(resolved.derived());

        dto.setSourcePath(upload.getFilePath());

        dto.setStorageBackend(upload.getStorageBackend());
        dto.setS3Bucket(upload.getS3Bucket());
        dto.setS3Key(upload.getS3Key());

        dto.setSizeBytes(upload.getSizeBytes());
        dto.setBytesTotal(upload.getBytesTotal());
        dto.setBytesUploaded(upload.getBytesUploaded());
        dto.setProgressPct(calcPct(upload.getBytesUploaded(), upload.getBytesTotal()));

        dto.setPartsTotal(upload.getPartsTotal());
        dto.setPartsCompleted(upload.getPartsCompleted());

        dto.setItemsCount(upload.getItems() != null ? upload.getItems().size() : 0);
        dto.setPartsCount(upload.getParts() != null ? upload.getParts().size() : 0);

        dto.setCreatedAt(upload.getCreatedAt());
        dto.setUpdatedAt(upload.getUpdatedAt());
        dto.setCompletedAt(upload.getCompletedAt());

        dto.setErrorCode(upload.getErrorCode());
        dto.setErrorMessage(upload.getErrorMessage());
        return dto;
    }

    private static Double calcPct(long num, long den) {
        if (den <= 0) {
            return null;
        }
        double pct = (num * 100.0) / den;
        // redondeo suave a 2 decimales
        return Math.round(pct * 100.0) / 100.0;
    }
}

