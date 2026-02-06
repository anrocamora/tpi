package es.tsystems.genomics.tpiagent.api.v1.dto;

import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;

import java.util.EnumMap;
import java.util.Map;

public class UploadsSummaryDto {
    private int total;
    private Map<UploadStatus, Long> byStatus = new EnumMap<>(UploadStatus.class);

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public Map<UploadStatus, Long> getByStatus() {
        return byStatus;
    }

    public void setByStatus(Map<UploadStatus, Long> byStatus) {
        this.byStatus = byStatus;
    }
}

