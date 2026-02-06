package es.tsystems.genomics.tpiagent.api.v1.support;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class RunIdResolver {

    private RunIdResolver() {
    }

    public static ResolvedRunId resolve(Upload upload, AgentUploadProperties props) {
        if (upload == null) {
            return new ResolvedRunId(null, false);
        }

        String runId = upload.getRunId();
        if (runId != null && !runId.isBlank()) {
            return new ResolvedRunId(runId, false);
        }

        // Intentar derivar de filePath como: <sourceDirectory>/<RUN_ID>/...
        String filePath = upload.getFilePath();
        if (filePath != null && !filePath.isBlank()) {
            String derived = deriveFromPath(filePath, props);
            if (derived != null && !derived.isBlank()) {
                return new ResolvedRunId(derived, true);
            }
        }

        // Fallback: intentar a partir del S3 key (primer segmento significativo)
        String s3Key = upload.getS3Key();
        if (s3Key != null && !s3Key.isBlank()) {
            String derived = deriveFromS3Key(s3Key);
            if (derived != null && !derived.isBlank()) {
                return new ResolvedRunId(derived, true);
            }
        }

        return new ResolvedRunId(null, false);
    }

    private static String deriveFromPath(String filePath, AgentUploadProperties props) {
        try {
            Path p = Paths.get(filePath).normalize();

            // 1) Intentar relativo respecto a inboxDirectory: <inbox>/<sourceName>/<agentId>/source/<runId>/...
            //    o bien <inbox>/<sourceName>/<runId>/... (runs sin mover aún)
            if (props != null && props.getInboxDirectory() != null) {
                Path inbox = Paths.get(props.getInboxDirectory()).normalize();
                if (p.startsWith(inbox) && p.getNameCount() > inbox.getNameCount()) {
                    // La estructura es: inbox/sourceName/agentId/source/runId/...
                    // o: inbox/sourceName/runId/... (antes de mover)
                    // Intentamos extraer el runId buscando después de "source" o después de sourceName

                    int inboxDepth = inbox.getNameCount();

                    // Buscar si hay un segmento "source" en el path
                    for (int i = inboxDepth; i < p.getNameCount(); i++) {
                        String segment = p.getName(i).toString();
                        if ("source".equals(segment) && i + 1 < p.getNameCount()) {
                            // El siguiente segmento es el runId
                            return p.getName(i + 1).toString();
                        }
                    }

                    // Si no hay "source", asumir que el runId está directamente después del sourceName
                    // (estructura: inbox/sourceName/runId/...)
                    if (p.getNameCount() > inboxDepth + 1) {
                        return p.getName(inboxDepth + 1).toString();
                    }
                }
            }

            // 2) Último recurso: si el path apunta a un archivo dentro del run, usar el parent
            // (evita devolver el nombre del archivo)
            Path parent = p.getParent();
            if (parent != null && parent.getFileName() != null) {
                return parent.getFileName().toString();
            }
        } catch (Exception ignored) {
            // best-effort
        }
        return null;
    }

    private static String deriveFromS3Key(String s3Key) {
        String normalized = s3Key.replace('\\', '/');
        String[] parts = normalized.split("/");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            // Evitar prefijos genéricos típicos
            if ("agent".equalsIgnoreCase(part) || "source".equalsIgnoreCase(part)) {
                continue;
            }
            return part;
        }
        return null;
    }

    public record ResolvedRunId(String runId, boolean derived) {
    }
}

