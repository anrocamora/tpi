package es.tsystems.genomics.tpiagent.api.v1.controller;

import es.tsystems.genomics.tpiagent.api.v1.dto.UploadDto;
import es.tsystems.genomics.tpiagent.api.v1.dto.UploadsPageDto;
import es.tsystems.genomics.tpiagent.api.v1.support.UploadDtoMapper;
import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import es.tsystems.genomics.tpiagent.upload.service.UploadStateStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/uploads")
@Tag(name = "Uploads", description = "Consulta read-only de uploads")
public class UploadsApiController {

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 500;

    private final UploadStateStore stateStore;
    private final AgentUploadProperties props;

    public UploadsApiController(UploadStateStore stateStore, AgentUploadProperties props) {
        this.stateStore = stateStore;
        this.props = props;
    }

    @GetMapping
    @Operation(summary = "Listar uploads", description = "Lista de uploads con filtros y paginación simple")
    public UploadsPageDto list(
            @Parameter(description = "Filtrar por estado (CSV). Ej: IN_PROGRESS,FAILED")
            @RequestParam(name = "status", required = false) String statusCsv,
            @Parameter(description = "Filtrar por runId exacto")
            @RequestParam(name = "runId", required = false) String runId,
            @Parameter(description = "Búsqueda simple sobre runId/filePath/s3Key")
            @RequestParam(name = "q", required = false) String q,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset
    ) {
        int safeLimit = sanitizeLimit(limit);
        int safeOffset = Math.max(offset != null ? offset : 0, 0);

        Set<UploadStatus> statuses = parseStatuses(statusCsv);

        List<Upload> filtered = stateStore.getAllUploads().values().stream()
                .filter(u -> statuses.isEmpty() || (u.getStatus() != null && statuses.contains(u.getStatus())))
                .filter(u -> runId == null || runId.isBlank() || matchesRunId(u, runId))
                .filter(u -> q == null || q.isBlank() || matchesQuery(u, q))
                .sorted(Comparator.comparing(Upload::getUpdatedAt, Comparator.nullsLast(Comparator.naturalOrder())).reversed())
                .toList();

        int total = filtered.size();

        List<UploadDto> pageItems = filtered.stream()
                .skip(safeOffset)
                .limit(safeLimit)
                .map(u -> UploadDtoMapper.toDto(u, props))
                .collect(Collectors.toList());

        UploadsPageDto dto = new UploadsPageDto();
        dto.setLimit(safeLimit);
        dto.setOffset(safeOffset);
        dto.setTotal(total);
        dto.setCount(pageItems.size());
        dto.setItems(pageItems);
        return dto;
    }

    @GetMapping("/{uploadId}")
    @Operation(summary = "Detalle de un upload", description = "Devuelve detalle (sin items/parts por ahora) del upload")
    public UploadDto getById(@PathVariable("uploadId") String uploadId) {
        Upload upload = stateStore.getAllUploads().get(uploadId);
        if (upload == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Upload not found: " + uploadId);
        }
        return UploadDtoMapper.toDto(upload, props);
    }

    private int sanitizeLimit(Integer limit) {
        int l = limit != null ? limit : DEFAULT_LIMIT;
        if (l < 1) {
            l = DEFAULT_LIMIT;
        }
        return Math.min(l, MAX_LIMIT);
    }

    private Set<UploadStatus> parseStatuses(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        Set<UploadStatus> out = new HashSet<>();
        String[] parts = csv.split(",");
        for (String p : parts) {
            String v = p.trim();
            if (v.isEmpty()) {
                continue;
            }
            try {
                out.add(UploadStatus.valueOf(v));
            } catch (IllegalArgumentException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid status: " + v);
            }
        }
        return out;
    }

    private boolean matchesRunId(Upload u, String runId) {
        UploadDto dto = UploadDtoMapper.toDto(u, props);
        return dto.getRunId() != null && dto.getRunId().equals(runId);
    }

    private boolean matchesQuery(Upload u, String q) {
        String needle = q.toLowerCase(Locale.ROOT);
        UploadDto dto = UploadDtoMapper.toDto(u, props);

        if (dto.getRunId() != null && dto.getRunId().toLowerCase(Locale.ROOT).contains(needle)) {
            return true;
        }
        if (u.getFilePath() != null && u.getFilePath().toLowerCase(Locale.ROOT).contains(needle)) {
            return true;
        }
        return u.getS3Key() != null && u.getS3Key().toLowerCase(Locale.ROOT).contains(needle);
    }
}

