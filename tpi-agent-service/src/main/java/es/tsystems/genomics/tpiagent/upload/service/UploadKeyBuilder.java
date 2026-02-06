package es.tsystems.genomics.tpiagent.upload.service;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Construye claves S3 y URLs de forma consistente.
 *
 * Clase sin estado (pura) para facilitar tests y reducir tamaño de UploadService.
 */
final class UploadKeyBuilder {

    private UploadKeyBuilder() {
    }

    static String buildKey(String basePath, String sourceName, String agentId, String relativePath) {
        String normalizedBase = basePath.endsWith("/") ? basePath : basePath + "/";
        return normalizedBase + sourceName + "/" + agentId + "/" + relativePath;
    }

    static String buildRunPrefixKey(String basePath, String sourceName, String agentId, String runId) {
        String normalizedBase = basePath.endsWith("/") ? basePath : basePath + "/";
        return normalizedBase + sourceName + "/" + agentId + "/" + runId;
    }

    static String buildRunItemKey(String basePath, String sourceName, String agentId, String runId, String relativePath) {
        return buildRunPrefixKey(basePath, sourceName, agentId, runId) + "/" + relativePath;
    }

    static String toS3Url(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

    static String relativizeToSourceRoot(String sourceDirectory, Path filePath) {
        Path sourceRoot;
        try {
            sourceRoot = Paths.get(sourceDirectory).normalize().toAbsolutePath();
        } catch (Exception e) {
            return filePath.getFileName().toString();
        }

        try {
            Path normalizedFile = filePath.normalize().toAbsolutePath();

            // Si el fichero no cuelga del sourceRoot (p.ej. otra unidad en Windows), evita rutas con "..".
            if (!normalizedFile.startsWith(sourceRoot)) {
                return normalizedFile.getFileName().toString();
            }

            Path rel = sourceRoot.relativize(normalizedFile);
            String relStr = normalizePathSeparators(rel.toString());
            if (relStr.startsWith("../") || relStr.equals("..")) {
                return normalizedFile.getFileName().toString();
            }
            return relStr;
        } catch (Exception e) {
            return filePath.getFileName().toString();
        }
    }

    static String normalizePathSeparators(String path) {
        return path.replace('\\', '/');
    }
}
