package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.stream.Stream;

@Component
public class DirectoryUploadScheduler {

    private static final Logger log = LoggerFactory.getLogger(DirectoryUploadScheduler.class);
    private static final String RUN_COMPLETION_FLAG = "RunCompletionStatus.xml";

    private final AgentUploadProperties properties;
    private final UploadService uploadService;
    private final UploadStateStore stateStore;
    private final SourceLoggerFactory sourceLoggerFactory;

    private volatile boolean recoveryCompleted = false;

    public DirectoryUploadScheduler(AgentUploadProperties properties,
                                   UploadService uploadService,
                                   UploadStateStore stateStore,
                                   SourceLoggerFactory sourceLoggerFactory) {
        this.properties = properties;
        this.uploadService = uploadService;
        this.stateStore = stateStore;
        this.sourceLoggerFactory = sourceLoggerFactory;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recoverPendingUploads() {
        log.info("==========================================");
        log.info("Starting recovery of pending uploads...");
        log.info("==========================================");

        // Create inbox directory if it doesn't exist
        createDirectoryIfNotExists(properties.getInboxDirectory(), "inbox");

        // Scan for sources (first-level subdirectories) and create their structure
        Path inboxPath = Paths.get(properties.getInboxDirectory());
        try (Stream<Path> sourceDirs = Files.list(inboxPath)) {
            sourceDirs.filter(Files::isDirectory)
                    .forEach(sourceDir -> {
                        String sourceName = sourceDir.getFileName().toString();
                        // Skip if it's the agent structure itself (sourceName/{agentId})
                        Path agentSubdir = sourceDir.resolve(properties.getAgentId());
                        if (Files.exists(agentSubdir)) {
                            // This is already a source with agent structure, create subdirectories
                            createDirectoryIfNotExists(properties.getSourceDirectoryFor(sourceName), "source [" + sourceName + "]");
                            createDirectoryIfNotExists(properties.getCompletedDirectoryFor(sourceName), "completed [" + sourceName + "]");
                            createDirectoryIfNotExists(properties.getFailedDirectoryFor(sourceName), "failed [" + sourceName + "]");
                            createDirectoryIfNotExists(properties.getLogsDirectoryFor(sourceName), "logs [" + sourceName + "]");
                            log.info("✓ Detected existing source: {}", sourceName);
                        } else {
                            // This is a new source, we'll create structure on-demand when processing runs
                            log.info("✓ Detected source (structure will be created on first run): {}", sourceName);
                        }
                    });
        } catch (IOException e) {
            log.error("Error scanning inbox directory for sources: {}", e.getMessage());
        }

        // Wait for state store to be ready
        stateStore.waitUntilReady();

        List<Upload> pendingUploads = stateStore.getAllUploads().values().stream()
                .filter(u -> u.getStatus() == UploadStatus.IN_PROGRESS)
                .toList();

        if (pendingUploads.isEmpty()) {
            log.info("✓ No pending uploads to recover");
            log.info("==========================================");

            recoveryCompleted = true;
            log.info("✓ Recovery process finished. Scheduler is now enabled to process new files.");
            return;
        }

        log.info("Found {} pending upload(s) to recover:", pendingUploads.size());
        for (Upload u : pendingUploads) {
            log.info("  - Upload ID: {} | RunID: {} | File: {} | Progress: {}/{} parts",
                u.getId(), u.getRunId(), u.getFilePath(), u.getPartsCompleted(), u.getPartsTotal());
        }

        // En el flujo actual de runs multi-source, los uploads IN_PROGRESS se recuperan automáticamente:
        // - Si el directorio del run sigue existiendo con el flag, el scheduler lo detectará y procesará
        // - Si el directorio ya no existe, abortamos el upload

        int aborted = 0;
        for (Upload upload : pendingUploads) {
            Path filePath = Paths.get(upload.getFilePath());
            if (!Files.exists(filePath)) {
                log.warn("❌ Directory/file {} for upload {} no longer exists, aborting",
                    upload.getFilePath(), upload.getId());
                try {
                    uploadService.abortUploadForMissingFile(upload);
                    aborted++;
                } catch (Exception e) {
                    log.error("Error aborting upload {}", upload.getId(), e);
                }
            } else {
                log.info("✓ Upload {} will be processed by scheduler (run: {}, path: {})",
                    upload.getId(), upload.getRunId(), upload.getFilePath());
            }
        }

        log.info("==========================================");
        log.info("Upload recovery completed: {} will be reprocessed, {} aborted",
            pendingUploads.size() - aborted, aborted);
        log.info("==========================================");

        recoveryCompleted = true;
        log.info("✓ Recovery process finished. Scheduler is now enabled to process new files.");

        log.info("Performing initial scan for new files...");
        scanAndUpload();
    }

    @Scheduled(fixedDelayString = "${agent.upload.scan-interval-ms:30000}")
    public void scanAndUpload() {
        if (!recoveryCompleted) {
            log.debug("Scheduler waiting for recovery to complete...");
            return;
        }

        stageInboxToSource();
        scanSourceAndUpload();
    }

    private void stageInboxToSource() {
        Path inbox = Paths.get(properties.getInboxDirectory());

        if (!Files.isDirectory(inbox)) {
            log.warn("Configured inbox directory {} is not a directory", inbox);
            return;
        }

        // Iterar sobre sources (subcarpetas de primer nivel en inbox)
        try (Stream<Path> sourceDirs = Files.list(inbox)) {
            sourceDirs.filter(Files::isDirectory)
                    .forEach(sourceDir -> {
                        String sourceName = sourceDir.getFileName().toString();

                        // Asegurarnos de que la estructura existe para este source
                        ensureSourceStructure(sourceName);

                        Path sourceRoot = Paths.get(properties.getSourceDirectoryFor(sourceName));
                        Path completedRoot = Paths.get(properties.getCompletedDirectoryFor(sourceName));
                        Path failedRoot = Paths.get(properties.getFailedDirectoryFor(sourceName));
                        Path logsRoot = Paths.get(properties.getLogsDirectoryFor(sourceName));

                        // Escanear runs directamente en la raíz del source (primer nivel bajo inbox/{sourceName})
                        // que NO estén ya en las carpetas de estructura del agente
                        if (properties.isMoveDirectoryDatasets()) {
                            // Modo PRE: mover datasets (directorios) completos
                            try (Stream<Path> items = Files.list(sourceDir)) {
                                items.filter(Files::isDirectory)
                                        .filter(dir -> !isUnder(dir, sourceRoot))
                                        .filter(dir -> !isUnder(dir, completedRoot))
                                        .filter(dir -> !isUnder(dir, failedRoot))
                                        .filter(dir -> !isUnder(dir, logsRoot))
                                        .forEach(dir -> tryMoveDirectoryDatasetIfStable(dir, sourceDir, sourceRoot, sourceName));
                            } catch (IOException e) {
                                log.error("Error listing source directory {} for datasets", sourceDir, e);
                            }

                            // También mover ficheros sueltos en la raíz del source
                            try (Stream<Path> items = Files.list(sourceDir)) {
                                items.filter(Files::isRegularFile)
                                        .forEach(p -> tryMoveFileIfStable(p, sourceDir, sourceRoot, sourceName));
                            } catch (IOException e) {
                                log.error("Error listing source directory {} for files", sourceDir, e);
                            }
                        } else {
                            // Modo normal: mover runs desde sourceDir a sourceRoot
                            try (Stream<Path> paths = Files.walk(sourceDir, Integer.MAX_VALUE, FileVisitOption.FOLLOW_LINKS)) {
                                paths.filter(Files::isRegularFile)
                                        .filter(p -> !isUnder(p, sourceRoot))
                                        .filter(p -> !isUnder(p, completedRoot))
                                        .filter(p -> !isUnder(p, failedRoot))
                                        .filter(p -> !isUnder(p, logsRoot))
                                        .forEach(p -> tryMoveFileIfStable(p, sourceDir, sourceRoot, sourceName));
                            } catch (IOException e) {
                                log.error("Error walking source directory {}", sourceDir, e);
                            }
                        }
                    });
        } catch (IOException e) {
            log.error("Error scanning inbox directory {} for sources", inbox, e);
        }
    }

    private void tryMoveDirectoryDatasetIfStable(Path datasetDir, Path inboxRoot, Path sourceRoot, String sourceName) {
        if (!Files.isDirectory(datasetDir)) {
            return;
        }

        // Obtener logger específico del source
        org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, properties.getLogsDirectoryFor(sourceName));

        // Flag obligatorio para iniciar el movimiento/subida del run
        Path flag = datasetDir.resolve(RUN_COMPLETION_FLAG);
        if (!Files.isRegularFile(flag)) {
            log.debug("Run folder {} does not contain {}, skipping", datasetDir, RUN_COMPLETION_FLAG);
            return;
        }

        if (!isDirectoryStable(datasetDir)) {
            log.debug("Directory dataset not stable yet, skipping this scan: {}", datasetDir);
            return;
        }

        // No mover si ya hay un upload activo para este run
        String runId = datasetDir.getFileName().toString();
        if (stateStore.findByRunId(runId).filter(u -> u.getStatus() == UploadStatus.IN_PROGRESS).isPresent()) {
            log.debug("Skipping run {} because an active upload exists", runId);
            return;
        }

        Path relative;
        try {
            relative = inboxRoot.relativize(datasetDir);
        } catch (IllegalArgumentException ex) {
            relative = datasetDir.getFileName();
        }

        Path destinationDir = sourceRoot.resolve(relative);

        try {
            Files.createDirectories(destinationDir.getParent());
            // MOVE (opción 1): intentar rename/move del directorio completo
            Files.move(datasetDir, destinationDir, StandardCopyOption.REPLACE_EXISTING);
            log.info("✓ Staged directory dataset into source [{}]: {} -> {}", sourceName, datasetDir, destinationDir);
            sourceLog.info("✓ Staged directory dataset: {} -> {}", datasetDir, destinationDir);
        } catch (IOException moveError) {
            // Fallback: copy + delete (move semántico) para casos SMB/permiso
            log.warn("Move failed for directory dataset {} [{}], falling back to copy+delete: {}",
                    datasetDir, sourceName, moveError.getMessage());
            sourceLog.warn("Move failed for directory dataset {}, falling back to copy+delete: {}",
                    datasetDir, moveError.getMessage());
            try {
                copyRecursively(datasetDir, destinationDir);
                deleteRecursively(datasetDir);
                log.info("✓ Staged directory dataset into source (copy+delete) [{}]: {} -> {}", sourceName, datasetDir, destinationDir);
                sourceLog.info("✓ Staged directory dataset (copy+delete): {} -> {}", datasetDir, destinationDir);
            } catch (IOException copyError) {
                log.error("Failed to stage directory dataset {} [{}] to {}: {}", datasetDir, sourceName, destinationDir, copyError.getMessage());
                sourceLog.error("Failed to stage directory dataset {} to {}: {}", datasetDir, destinationDir, copyError.getMessage());
            }
        }
    }

    private void tryMoveFileIfStable(Path file, Path inboxRoot, Path sourceRoot, String sourceName) {
        // Obtener logger específico del source
        org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, properties.getLogsDirectoryFor(sourceName));

        // No mover si ya hay un upload activo para este path
        if (stateStore.hasActiveUploadForPath(file.toString())) {
            log.debug("Skipping {} because an active upload exists", file);
            return;
        }

        if (!isStable(file)) {
            log.debug("File not stable yet, skipping this scan: {}", file);
            return;
        }

        Path relative;
        try {
            relative = inboxRoot.relativize(file);
        } catch (IllegalArgumentException ex) {
            relative = file.getFileName();
        }

        Path destination = sourceRoot.resolve(relative);

        try {
            Files.createDirectories(destination.getParent());
            Files.move(file, destination, StandardCopyOption.REPLACE_EXISTING);
            log.info("✓ Staged file into source [{}]: {} -> {}", sourceName, file, destination);
            sourceLog.info("✓ Staged file: {} -> {}", file, destination);
        } catch (IOException e) {
            log.error("Failed to move {} [{}] to source {}: {}", file, sourceName, destination, e.getMessage());
            sourceLog.error("Failed to move {} to source {}: {}", file, destination, e.getMessage());
        }
    }

    private boolean isDirectoryStable(Path dir) {
        long windowMs = properties.getFileStabilityWindowMs();
        final long[] newestMtime = {0L};
        final boolean[] sawFile = {false};

        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!attrs.isRegularFile()) {
                        return FileVisitResult.CONTINUE;
                    }
                    sawFile[0] = true;

                    // Si hay uploads activos dentro del dataset, no lo movemos
                    if (stateStore.hasActiveUploadForPath(file.toString())) {
                        return FileVisitResult.TERMINATE;
                    }

                    FileTime mtime = Files.getLastModifiedTime(file);
                    newestMtime[0] = Math.max(newestMtime[0], mtime.toMillis());

                    // si un fichero no es estable, el directorio no lo es
                    if (!isStable(file)) {
                        return FileVisitResult.TERMINATE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });

            // Si estaba vacío, no lo consideramos dataset.
            if (!sawFile[0]) {
                return false;
            }

            long ageMs = Math.max(0L, System.currentTimeMillis() - newestMtime[0]);
            return ageMs >= windowMs;
        } catch (IOException e) {
            log.debug("Unable to inspect directory {}, skipping: {}", dir, e.getMessage());
            return false;
        }
    }

    private boolean isStable(Path file) {
        long windowMs = properties.getFileStabilityWindowMs();
        try {
            FileTime mtime = Files.getLastModifiedTime(file);
            long size = Files.size(file);

            long ageMs = Math.max(0L, System.currentTimeMillis() - mtime.toMillis());
            if (ageMs < windowMs) {
                return false;
            }

            // Doble lectura rápida para detectar cambios de tamaño/mtime durante el scan
            FileTime mtime2 = Files.getLastModifiedTime(file);
            long size2 = Files.size(file);
            return mtime.equals(mtime2) && size == size2;
        } catch (IOException e) {
            log.debug("Unable to stat file {}, skipping: {}", file, e.getMessage());
            return false;
        }
    }

    private void scanSourceAndUpload() {
        Path inboxPath = Paths.get(properties.getInboxDirectory());
        if (!Files.isDirectory(inboxPath)) {
            log.warn("Configured inbox directory {} is not a directory", inboxPath);
            return;
        }

        // Iterar sobre sources (subcarpetas de primer nivel en inbox)
        try (Stream<Path> sourceDirs = Files.list(inboxPath)) {
            sourceDirs.filter(Files::isDirectory)
                    .forEach(sourceDir -> {
                        String sourceName = sourceDir.getFileName().toString();
                        // Crear estructura de directorios para este source si no existe
                        ensureSourceStructure(sourceName);

                        // Obtener logger específico del source
                        org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, properties.getLogsDirectoryFor(sourceName));

                        // Escanear runs en el directorio source de este source
                        Path sourceDirectory = Paths.get(properties.getSourceDirectoryFor(sourceName));
                        if (!Files.isDirectory(sourceDirectory)) {
                            log.debug("Source directory for {} does not exist yet: {}", sourceName, sourceDirectory);
                            return;
                        }

                        // Subir runs del source (primer nivel bajo {source}/{agentId}/source) solo si tienen flag
                        try (Stream<Path> firstLevel = Files.list(sourceDirectory)) {
                            firstLevel.filter(Files::isDirectory)
                                    .filter(runDir -> Files.isRegularFile(runDir.resolve(RUN_COMPLETION_FLAG)))
                                    .forEach(runDir -> {
                                        sourceLog.info("Starting upload for run: {}", runDir.getFileName());
                                        uploadService.uploadRunFolder(runDir, sourceName);
                                    });
                        } catch (IOException e) {
                            log.error("Error scanning source directory {} for source {}", sourceDirectory, sourceName, e);
                            sourceLog.error("Error scanning source directory {}", sourceDirectory, e);
                        }
                    });
        } catch (IOException e) {
            log.error("Error scanning inbox directory {}", inboxPath, e);
        }
    }

    private void ensureSourceStructure(String sourceName) {
        createDirectoryIfNotExists(properties.getSourceDirectoryFor(sourceName), "source [" + sourceName + "]");
        createDirectoryIfNotExists(properties.getCompletedDirectoryFor(sourceName), "completed [" + sourceName + "]");
        createDirectoryIfNotExists(properties.getFailedDirectoryFor(sourceName), "failed [" + sourceName + "]");
        createDirectoryIfNotExists(properties.getLogsDirectoryFor(sourceName), "logs [" + sourceName + "]");
    }

    private void copyRecursively(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path rel = source.relativize(dir);
                Path dst = target.resolve(rel);
                Files.createDirectories(dst);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path rel = source.relativize(file);
                Path dst = target.resolve(rel);
                Files.createDirectories(dst.getParent());
                Files.copy(file, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteRecursively(Path root) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private boolean isUnder(Path p, Path maybeParent) {
        if (p == null || maybeParent == null) return false;
        try {
            return p.normalize().startsWith(maybeParent.normalize());
        } catch (Exception e) {
            return false;
        }
    }

    private void createDirectoryIfNotExists(String directory, String label) {
        try {
            Path path = Paths.get(directory);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                log.info("📁 Created {} directory: {}", label, directory);
            }
        } catch (IOException e) {
            log.error("Failed to create {} directory {}: {}", label, directory, e.getMessage());
        }
    }

    // Método visible para testing
    void setRecoveryCompleted(boolean completed) {
        this.recoveryCompleted = completed;
    }
}
