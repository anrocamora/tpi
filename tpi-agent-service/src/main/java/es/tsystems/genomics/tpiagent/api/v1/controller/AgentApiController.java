package es.tsystems.genomics.tpiagent.api.v1.controller;

import es.tsystems.genomics.tpiagent.api.v1.dto.AgentStatusDto;
import es.tsystems.genomics.tpiagent.api.v1.dto.UploadsSummaryDto;
import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import es.tsystems.genomics.tpiagent.upload.service.UploadStateStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.EnumMap;

@RestController
@RequestMapping("/api/v1/agent")
@Tag(name = "Agent", description = "Endpoints read-only del agente")
public class AgentApiController {

    private final AgentUploadProperties props;
    private final UploadStateStore stateStore;

    public AgentApiController(AgentUploadProperties props, UploadStateStore stateStore) {
        this.props = props;
        this.stateStore = stateStore;
    }

    @GetMapping("/status")
    @Operation(summary = "Estado del agente", description = "Devuelve configuración efectiva no sensible y agregados del estado de uploads")
    public AgentStatusDto status() {
        AgentStatusDto dto = new AgentStatusDto();
        dto.setNow(Instant.now());

        dto.setAgentId(props.getAgentId());
        dto.setClient(props.getClient());
        dto.setEnv(props.getEnv());
        // sourceName ya no existe - es dinámico por source

        AgentStatusDto.DirectoriesDto dirs = new AgentStatusDto.DirectoriesDto();
        dirs.setInbox(props.getInboxDirectory());
        // Los directorios source, completed, failed ahora son dinámicos por source
        dto.setDirectories(dirs);

        dto.setStorageBackendId(props.getStorageBackendId());
        dto.setEventsTopic(props.getEffectiveEventsTopic());
        dto.setStateTopic(props.getEffectiveStateTopic());

        dto.setScanIntervalMs(props.getScanIntervalMs());
        dto.setFileStabilityWindowMs(props.getFileStabilityWindowMs());
        dto.setMoveDirectoryDatasets(props.isMoveDirectoryDatasets());

        dto.setUploads(summary());
        return dto;
    }

    @GetMapping("/uploads/summary")
    @Operation(summary = "Resumen de uploads", description = "Conteos por estado")
    public UploadsSummaryDto summary() {
        var uploads = stateStore.getAllUploads().values();

        UploadsSummaryDto dto = new UploadsSummaryDto();
        dto.setTotal(uploads.size());

        EnumMap<UploadStatus, Long> byStatus = new EnumMap<>(UploadStatus.class);
        for (UploadStatus status : UploadStatus.values()) {
            byStatus.put(status, 0L);
        }
        uploads.forEach(u -> {
            UploadStatus s = u.getStatus();
            if (s != null) {
                byStatus.put(s, byStatus.getOrDefault(s, 0L) + 1L);
            }
        });
        dto.setByStatus(byStatus);
        return dto;
    }
}
