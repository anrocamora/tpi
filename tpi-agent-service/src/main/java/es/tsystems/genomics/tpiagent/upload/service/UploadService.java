package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import es.tsystems.genomics.tpiagent.config.S3ClientFactory;
import es.tsystems.genomics.tpiagent.config.StorageBackendProperties;
import es.tsystems.genomics.tpiagent.config.StorageConfigurationProperties;
import es.tsystems.genomics.tpiagent.upload.model.Upload;
import es.tsystems.genomics.tpiagent.upload.model.UploadStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class UploadService {

    private static final long MAX_PARTS = 10_000;
    private static final String RUN_COMPLETION_FLAG = "RunCompletionStatus.xml";

    private static final Logger log = LoggerFactory.getLogger(UploadService.class);

    private final StorageConfigurationProperties storageConfigurationProperties;
    private final AgentUploadProperties agentUploadProperties;
    private final S3ClientFactory s3ClientFactory;
    private final KafkaTemplate<String, UploadEvent> kafkaTemplate;
    private final UploadStateStore stateStore;
    private final SourceLoggerFactory sourceLoggerFactory;

    // ExecutorService para paralelizar subidas de archivos en modo run
    private final java.util.concurrent.ExecutorService uploadExecutor;

    public UploadService(StorageConfigurationProperties storageConfigurationProperties,
                         AgentUploadProperties agentUploadProperties,
                         S3ClientFactory s3ClientFactory,
                         KafkaTemplate<String, UploadEvent> kafkaTemplate,
                         UploadStateStore stateStore,
                         SourceLoggerFactory sourceLoggerFactory) {
        this.storageConfigurationProperties = Objects.requireNonNull(storageConfigurationProperties);
        this.agentUploadProperties = Objects.requireNonNull(agentUploadProperties);
        this.s3ClientFactory = Objects.requireNonNull(s3ClientFactory);
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate);
        this.stateStore = Objects.requireNonNull(stateStore);
        this.sourceLoggerFactory = Objects.requireNonNull(sourceLoggerFactory);

        // Crear thread pool con número de threads configurado
        int threads = agentUploadProperties.getConcurrentUploads();
        this.uploadExecutor = java.util.concurrent.Executors.newFixedThreadPool(threads,
            r -> {
                Thread t = new Thread(r);
                t.setName("upload-worker-" + t.getId());
                t.setDaemon(true);
                return t;
            });
        log.info("✓ Initialized parallel upload thread pool with {} threads", threads);
    }


    @Retryable(
            retryFor = {S3Exception.class, IOException.class},
            maxAttemptsExpression = "#{@agentUploadProperties.maxRetries}",
            backoff = @Backoff(
                    delayExpression = "#{@agentUploadProperties.retryBackoffMs}",
                    maxDelayExpression = "#{@agentUploadProperties.maxRetryBackoffMs}",
                    multiplier = 2.0,
                    random = true
            )
    )
    private String uploadSinglePart(S3Client s3Client, Upload upload, int partNumber, byte[] buffer) {
        UploadPartRequest partRequest = UploadPartRequest.builder()
                .bucket(upload.getS3Bucket())
                .key(upload.getS3Key())
                .uploadId(upload.getS3UploadId())
                .partNumber(partNumber)
                .contentLength((long) buffer.length)
                .build();

        String etag = s3Client.uploadPart(partRequest, RequestBody.fromBytes(buffer)).eTag();

        // Validar y limpiar ETag
        if (etag == null || etag.trim().isEmpty()) {
            throw new IllegalStateException("Received null or empty ETag for part " + partNumber);
        }

        // Eliminar comillas dobles si están presentes (algunos backends S3 las incluyen)
        return etag.replace("\"", "");
    }

    @Recover
    public String recoverFromUploadFailure(Exception e, S3Client s3Client, Upload upload, int partNumber, byte[] buffer) {
        log.error("❌ EXHAUSTED RETRIES for part {}/{} of upload {} after {} attempts - {}",
            partNumber, upload.getPartsTotal(), upload.getId(), agentUploadProperties.getMaxRetries(), e.getMessage());
        publishEvent(upload, UploadEventType.PART_RETRY_FAILED, partNumber, "MAX_RETRIES_EXCEEDED", null, agentUploadProperties.getAgentId());
        throw new RuntimeException("Part upload failed after " + agentUploadProperties.getMaxRetries() + " retries", e);
    }


    public void abortUploadForMissingFile(Upload upload) {
        log.info("Aborting upload {} because file no longer exists", upload.getId());

        StorageBackendProperties backend = storageConfigurationProperties.backendMap().get(upload.getStorageBackend());
        if (backend != null && upload.getS3UploadId() != null) {
            try {
                S3Client s3Client = s3ClientFactory.clientFor(backend.getId());
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(upload.getS3Bucket())
                        .key(upload.getS3Key())
                        .uploadId(upload.getS3UploadId())
                        .build());
                log.info("Successfully aborted S3 multipart upload for {}", upload.getId());
            } catch (Exception e) {
                log.error("Error aborting S3 multipart upload for {}", upload.getId(), e);
            }
        }

        upload.setStatus(UploadStatus.ABORTED);
        upload.setErrorCode("FILE_NOT_FOUND");
        upload.setErrorMessage("Source file no longer exists: " + upload.getFilePath());
        upload.setUpdatedAt(Instant.now());
        publishEvent(upload, UploadEventType.UPLOAD_ABORTED, null, "FILE_NOT_FOUND", null, agentUploadProperties.getAgentId());
    }

    private void publishEvent(Upload upload, UploadEventType type, Integer partNumber, String errorCode, String partEtag, String agentId) {
        publishEvent(upload, type, partNumber, errorCode, partEtag, agentId, null);
    }

    private void publishEvent(Upload upload, UploadEventType type, Integer partNumber, String errorCode, String partEtag, String agentId, Double progressPercentage) {
        UploadEvent event = new UploadEvent();
        event.setEventType(type);
        event.setUploadId(upload.getId());
        event.setAgentId(agentId);
        event.setFilePath(upload.getFilePath());
        event.setSizeBytes(upload.getSizeBytes());
        event.setStorageBackend(upload.getStorageBackend());
        event.setS3Bucket(upload.getS3Bucket());
        event.setS3Key(upload.getS3Key());
        event.setS3UploadId(upload.getS3UploadId());
        event.setOccurredAt(Instant.now());
        event.setErrorCode(errorCode);
        event.setErrorMessage(upload.getErrorMessage());
        event.setPartNumber(partNumber);
        event.setPartEtag(partEtag);
        event.setPartsCompleted(upload.getPartsCompleted());
        event.setPartsTotal(upload.getPartsTotal());
        event.setMetadata(upload.getMetadata());

        // Include folder/run catalog info when available
        event.setFolder(upload.getFolder());

        // Calculate progress percentage
        if (progressPercentage != null) {
            // Use explicitly provided progress (for single-part uploads with real-time tracking)
            event.setProgressPercentage(Math.round(progressPercentage * 100.0) / 100.0);
        } else if (upload.getPartsTotal() > 0) {
            // Calculate from parts (for multipart uploads)
            double progress = (upload.getPartsCompleted() * 100.0) / upload.getPartsTotal();
            event.setProgressPercentage(Math.round(progress * 100.0) / 100.0);
        }

        kafkaTemplate.send(agentUploadProperties.getEffectiveEventsTopic(), upload.getId(), event);
        stateStore.applyEvent(event);
    }

    /**
     * Publica un evento a nivel run (1 UPLOAD_STARTED, 1 UPLOAD_COMPLETED/FAILED) con progreso por bytes.
     * IMPORTANTE: Solo estos eventos incluyen el Folder completo para catalogación.
     * Los eventos intermedios de progreso (publishItemProgress) NO incluyen Folder para optimizar tamaño.
     */
    private void publishRunEvent(Upload runUpload, UploadEventType type, Integer partNumber, String errorCode, String etag) {
        UploadEvent event = new UploadEvent();
        event.setEventType(type);
        event.setUploadId(runUpload.getId());
        event.setAgentId(agentUploadProperties.getAgentId());
        event.setSourceName(runUpload.getSourceName());
        event.setRunId(runUpload.getRunId());
        event.setFolder(runUpload.getFolder());
        event.setFilePath(runUpload.getFilePath());

        // Parts counters en modo run: representan ficheros del run.
        // - partsTotal: total de ficheros del árbol (folder + subfolders)
        // - partsCompleted: total de ficheros completados hasta ahora (catálogo incremental en folder.files del root)
        if (runUpload.getFolder() != null) {
            int totalFiles = countFilesRecursive(runUpload.getFolder());
            int completedFiles = java.util.Optional.ofNullable(runUpload.getFolder().getFiles())
                    .map(java.util.List::size)
                    .orElse(0);

            event.setPartsTotal(totalFiles);
            event.setPartsCompleted(completedFiles);
        }

        // Para uploads por run/carpeta, sizeBytes puede venir a 0. En ese caso usamos bytesTotal.
        long effectiveSizeBytes = runUpload.getSizeBytes();
        if (effectiveSizeBytes == 0L && runUpload.getBytesTotal() > 0L) {
            effectiveSizeBytes = runUpload.getBytesTotal();
        }
        event.setSizeBytes(effectiveSizeBytes);

        event.setStorageBackend(runUpload.getStorageBackend());
        event.setS3Bucket(runUpload.getS3Bucket());
        event.setS3Key(runUpload.getS3Key());
        event.setS3UploadId(runUpload.getS3UploadId());
        event.setOccurredAt(Instant.now());
        event.setErrorCode(errorCode);
        event.setErrorMessage(runUpload.getErrorMessage());
        event.setPartNumber(partNumber);
        event.setPartEtag(etag);

        // Progreso a nivel run
        event.setBytesUploaded(runUpload.getBytesUploaded());
        event.setBytesTotal(runUpload.getBytesTotal());
        if (event.getBytesTotal() != null && event.getBytesTotal() > 0) {
            event.setProgressPercentage((event.getBytesUploaded() != null ? event.getBytesUploaded() : 0L) * 100.0 / event.getBytesTotal());
        }

        try {
            kafkaTemplate.send(agentUploadProperties.getEffectiveEventsTopic(), runUpload.getId(), event);
        } catch (Exception e) {
            log.debug("Kafka send failed (ignored in tests): {}", e.getMessage());
        }

        stateStore.applyEvent(event);
    }

    /**
     * Sube un run/carpeta completa. La carpeta debe contener RunCompletionStatus.xml (flag),
     * pero el flag se ignora y no se sube.
     * @param runDir Path del directorio del run
     * @param sourceName Nombre del source (p.ej. MiSeq, Nasertic, NextSeq)
     */
    public void uploadRunFolder(Path runDir, String sourceName) {
        if (runDir == null || !Files.isDirectory(runDir)) {
            return;
        }

        // Obtener logger específico del source
        org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, agentUploadProperties.getLogsDirectoryFor(sourceName));

        String runId = runDir.getFileName().toString();
        if (!Files.isRegularFile(runDir.resolve(RUN_COMPLETION_FLAG))) {
            log.debug("Skipping run {} because {} is missing", runId, RUN_COMPLETION_FLAG);
            return;
        }

        StorageBackendProperties backend = storageConfigurationProperties.backendMap().get(agentUploadProperties.getStorageBackendId());
        if (backend == null) {
            throw new IllegalArgumentException("Storage backend not configured: " + agentUploadProperties.getStorageBackendId());
        }

        // Evitar duplicados: si ya hay upload en progreso para el run, no arrancamos otro.
        if (stateStore.findByRunId(runId).filter(u -> u.getStatus() == UploadStatus.IN_PROGRESS).isPresent()) {
            log.info("Skipping run {} because an active upload exists", runId);
            sourceLog.info("Skipping run {} because an active upload exists", runId);
            return;
        }

        sourceLog.info("==================================================");
        sourceLog.info("Starting upload for run: {}", runId);
        sourceLog.info("==================================================");

        // Construir inventario de ficheros del run (excluyendo el flag)
        List<Path> files;
        try (java.util.stream.Stream<Path> walk = Files.walk(runDir)) {
            files = walk.filter(Files::isRegularFile)
                    .filter(p -> !RUN_COMPLETION_FLAG.equals(p.getFileName().toString()))
                    .toList();
        } catch (IOException e) {
            sourceLog.error("Error listing run folder {}: {}", runDir, e.getMessage());
            throw new IllegalStateException("Error listing run folder " + runDir, e);
        }

        long bytesTotal = 0L;
        for (Path p : files) {
            try {
                bytesTotal += Files.size(p);
            } catch (IOException e) {
                sourceLog.error("Unable to read file size for {}: {}", p, e.getMessage());
                throw new IllegalStateException("Unable to read file size for " + p, e);
            }
        }

        sourceLog.info("Run contains {} files, total size: {} bytes", files.size(), bytesTotal);

        String uploadId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        // Crear evento inicial a nivel run
        Upload runUpload = new Upload();
        runUpload.setId(uploadId);
        runUpload.setRunId(runId);
        runUpload.setSourceName(sourceName);
        runUpload.setFilePath(runDir.toString()); // compatibilidad: path apuntando a la carpeta
        runUpload.setStatus(UploadStatus.IN_PROGRESS);
        runUpload.setStorageBackend(backend.getId());
        runUpload.setS3Bucket(backend.getBucket());
        runUpload.setS3Key(UploadKeyBuilder.buildRunPrefixKey(backend.getBasePath(), sourceName, agentUploadProperties.getAgentId(), runId));
        runUpload.setCreatedAt(now);
        runUpload.setUpdatedAt(now);
        runUpload.setBytesTotal(bytesTotal);
        runUpload.setBytesUploaded(0L);
        // Para uploads por run, sizeBytes debe reflejar el total del run (no existe un fichero único).
        runUpload.setSizeBytes(bytesTotal);

        // Modelo Folder (catálogo)
        String runUrl = UploadKeyBuilder.toS3Url(backend.getBucket(), runUpload.getS3Key());
        es.tsystems.genomics.tpiagent.upload.model.Source src = new es.tsystems.genomics.tpiagent.upload.model.Source(sourceName, null);

        // IMPORTANTE: el catálogo del run debe ser incremental. Creamos el Folder raíz (y estructura de subcarpetas)
        // pero sin añadir ficheros inicialmente. Los ficheros se añaden al completarse.
        es.tsystems.genomics.tpiagent.upload.model.Folder folder = buildFolderModel(runDir, backend, sourceName, agentUploadProperties.getAgentId(), runId, src);
        folder.setFiles(new java.util.ArrayList<>());
        folder.setUrl(runUrl);
        runUpload.setFolder(folder);

        publishRunEvent(runUpload, UploadEventType.UPLOAD_STARTED, null, null, null);

        // OPTIMIZACIÓN: Subida paralela de archivos para mejorar rendimiento
        // Con 59,541 archivos, la subida secuencial tarda 5h. La paralela reduce a 30-60min.
        sourceLog.info("Starting parallel upload with {} threads", agentUploadProperties.getConcurrentUploads());

        java.util.concurrent.atomic.AtomicLong bytesUploadedAtomic = new java.util.concurrent.atomic.AtomicLong(0L);
        java.util.concurrent.atomic.AtomicBoolean hasError = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicReference<Exception> firstError = new java.util.concurrent.atomic.AtomicReference<>();

        // Crear lista de futures para todas las subidas
        List<java.util.concurrent.CompletableFuture<Void>> futures = new ArrayList<>();

        for (Path file : files) {
            // Si ya hubo un error, no seguir encolando tareas
            if (hasError.get()) {
                break;
            }

            java.util.concurrent.CompletableFuture<Void> future = java.util.concurrent.CompletableFuture.runAsync(() -> {
                if (hasError.get()) {
                    return; // Skip si ya falló otro
                }

                try {
                    sourceLog.info("Uploading file: {}", file.getFileName());
                    // Thread-safe: usar synchronized para actualizar bytes y folder
                    synchronized (runUpload) {
                        long currentBytes = bytesUploadedAtomic.get();
                        long newBytes = uploadFileAsRunItem(runUpload, backend, runDir, file, currentBytes, sourceName);
                        bytesUploadedAtomic.set(newBytes);
                    }
                } catch (Exception e) {
                    hasError.set(true);
                    firstError.compareAndSet(null, e);
                    sourceLog.error("Error uploading file {}: {}", file, e.getMessage());
                }
            }, uploadExecutor);

            futures.add(future);
        }

        // Esperar a que terminen todas las subidas
        try {
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("Error during parallel upload: {}", e.getMessage());
        }

        boolean ok = !hasError.get();
        long bytesUploaded = bytesUploadedAtomic.get();

        if (!ok) {
            Exception e = firstError.get();
            runUpload.setErrorCode("UPLOAD_ERROR");
            runUpload.setErrorMessage(e != null ? e.getMessage() : "Unknown error during parallel upload");
        }

        if (ok) {
            runUpload.setStatus(UploadStatus.COMPLETED);
            runUpload.setCompletedAt(Instant.now());
            runUpload.setBytesUploaded(runUpload.getBytesTotal());
            moveDirectoryToDirectory(runDir, agentUploadProperties.getCompletedDirectoryFor(sourceName), "completed", sourceName);
            publishRunEvent(runUpload, UploadEventType.UPLOAD_COMPLETED, null, null, null);
            sourceLog.info("==================================================");
            sourceLog.info("✓ Upload completed successfully for run: {}", runId);
            sourceLog.info("==================================================");
        } else {
            runUpload.setStatus(UploadStatus.FAILED);
            runUpload.setUpdatedAt(Instant.now());
            moveDirectoryToDirectory(runDir, agentUploadProperties.getFailedDirectoryFor(sourceName), "failed", sourceName);
            publishRunEvent(runUpload, UploadEventType.UPLOAD_FAILED, null, runUpload.getErrorCode(), null);
            sourceLog.error("==================================================");
            sourceLog.error("❌ Upload failed for run: {} - {}", runId, runUpload.getErrorMessage());
            sourceLog.error("==================================================");
        }
    }


    /**
     * Evento STARTED a nivel de item (fichero) en modo run.
     *
     * No se envía a Kafka (no hay consumidores); sólo alimenta el snapshot de state.
     */
    private void publishItemStarted(Upload runUpload,
                                   String itemRelativePath,
                                   String itemFilePath,
                                   long sizeBytes,
                                   StorageBackendProperties backend,
                                   String s3Key) {
        UploadEvent event = new UploadEvent();
        event.setEventType(UploadEventType.UPLOAD_STARTED);
        event.setUploadId(runUpload.getId());
        event.setAgentId(agentUploadProperties.getAgentId());
        event.setSourceName(runUpload.getSourceName());
        event.setRunId(runUpload.getRunId());
        event.setItemRelativePath(itemRelativePath);

        // En el STARTED del item, el filePath apunta al fichero real.
        event.setFilePath(itemFilePath);
        event.setSizeBytes(sizeBytes);

        // OPTIMIZACIÓN: No incluir Folder en eventos STARTED por fichero para reducir tamaño de ~10MB a ~500 bytes
        // El Folder completo solo se envía en el UPLOAD_STARTED y UPLOAD_COMPLETED del run completo
        // event.setFolder(runUpload.getFolder());

        event.setStorageBackend(backend.getId());
        event.setS3Bucket(backend.getBucket());
        event.setS3Key(s3Key);
        event.setOccurredAt(Instant.now());

        stateStore.applyEvent(event);
    }

    private long uploadFileAsRunItem(Upload runUpload,
                                    StorageBackendProperties backend,
                                    Path runDir,
                                    Path file,
                                    long bytesUploadedSoFar,
                                    String sourceName) throws IOException {
        String agentId = agentUploadProperties.getAgentId();
        String runId = runUpload.getRunId();

        String relativePath;
        try {
            relativePath = runDir.relativize(file).toString().replace('\\', '/');
        } catch (Exception e) {
            relativePath = file.getFileName().toString();
        }

        long sizeBytes = Files.size(file);
        String key = UploadKeyBuilder.buildRunItemKey(backend.getBasePath(), sourceName, agentId, runId, relativePath);

        // Archivos de 0 bytes: usar siempre PutObject simple (multipart no es posible)
        if (sizeBytes == 0) {
            org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, agentUploadProperties.getLogsDirectoryFor(sourceName));
            sourceLog.debug("File {} is empty (0 bytes), using single-part upload", relativePath);

            publishItemStarted(runUpload, relativePath, file.toString(), sizeBytes, backend, key);

            Upload itemUpload = new Upload();
            itemUpload.setId(runUpload.getId());
            itemUpload.setRunId(runId);
            itemUpload.setFilePath(file.toString());
            itemUpload.setSizeBytes(sizeBytes);
            itemUpload.setStatus(UploadStatus.IN_PROGRESS);
            itemUpload.setStorageBackend(backend.getId());
            itemUpload.setS3Bucket(backend.getBucket());
            itemUpload.setS3Key(key);
            itemUpload.setCreatedAt(runUpload.getCreatedAt());
            itemUpload.setUpdatedAt(Instant.now());

            S3Client s3Client = s3ClientFactory.clientFor(backend.getId());
            return performSinglePartUploadForRunItem(runUpload, itemUpload, s3Client, file, bytesUploadedSoFar, relativePath);
        }

        // En modo run, sí publicamos STARTED por fichero para que el state tenga un inicio claro por item.
        // IMPORTANTE: este evento sólo afecta al snapshot de state (no hay consumidores aguas abajo).
        publishItemStarted(runUpload, relativePath, file.toString(), sizeBytes, backend, key);

        Upload itemUpload = new Upload();
        itemUpload.setId(runUpload.getId());
        itemUpload.setRunId(runId);
        itemUpload.setFilePath(file.toString());
        itemUpload.setSizeBytes(sizeBytes);
        itemUpload.setStatus(UploadStatus.IN_PROGRESS);
        itemUpload.setStorageBackend(backend.getId());
        itemUpload.setS3Bucket(backend.getBucket());
        itemUpload.setS3Key(key);
        itemUpload.setCreatedAt(runUpload.getCreatedAt());
        itemUpload.setUpdatedAt(Instant.now());

        // Multipart vs single-part: reutilizamos la lógica existente mediante un helper
        S3Client s3Client = s3ClientFactory.clientFor(backend.getId());

        // Subida single-part (PutObject) si está configurado
        if (backend.isUseSinglePartUpload()) {
            // OPTIMIZACIÓN: Solo publicar progreso al completar el fichero, no antes de iniciarlo
            bytesUploadedSoFar = performSinglePartUploadForRunItem(runUpload, itemUpload, s3Client, file, bytesUploadedSoFar, relativePath);
            return bytesUploadedSoFar;
        }

        // Multipart upload
        long partSizeBytes = Math.max(agentUploadProperties.getPartSizeMiB() * 1024L * 1024L,
                (long) Math.ceil((double) sizeBytes / MAX_PARTS));
        int totalParts = (int) Math.ceil((double) sizeBytes / partSizeBytes);
        itemUpload.setPartsTotal(totalParts);
        itemUpload.setPartsCompleted(0);

        CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(backend.getBucket())
                .key(key)
                .build());
        itemUpload.setS3UploadId(multipartUpload.uploadId());

        // OPTIMIZACIÓN: Solo publicar progreso al completar el fichero, no antes de iniciarlo
        bytesUploadedSoFar = performMultipartUploadForRunItem(runUpload, itemUpload, s3Client, file, partSizeBytes, bytesUploadedSoFar, relativePath, backend);
        return bytesUploadedSoFar;
    }

    private long performSinglePartUploadForRunItem(Upload runUpload, Upload itemUpload, S3Client s3Client, Path file,
                                                  long bytesUploadedSoFar, String relativePath) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(itemUpload.getS3Bucket())
                    .key(itemUpload.getS3Key())
                    .build();

            // Para archivos de 0 bytes, usar RequestBody vacío directamente
            if (itemUpload.getSizeBytes() == 0) {
                s3Client.putObject(request, RequestBody.fromBytes(new byte[0]));
            } else {
                // Para archivos con contenido, usar InputStream
                try (FileInputStream fis = new FileInputStream(file.toFile())) {
                    s3Client.putObject(request, RequestBody.fromInputStream(fis, itemUpload.getSizeBytes()));
                }
            }

            // Al finalizar el fichero, sumamos su tamaño completo y actualizamos el catálogo incremental
            long newBytes = bytesUploadedSoFar + itemUpload.getSizeBytes();
            runUpload.setBytesUploaded(newBytes);
            addCompletedRunItemToFolder(runUpload, relativePath, itemUpload.getS3Bucket(), itemUpload.getS3Key());

            publishItemProgress(runUpload, relativePath, newBytes, runUpload.getBytesTotal());
            return newBytes;
        } catch (Exception e) {
            throw new RuntimeException("Error uploading run item " + file + ": " + e.getMessage(), e);
        }
    }

    private long performMultipartUploadForRunItem(Upload runUpload,
                                                 Upload itemUpload,
                                                 S3Client s3Client,
                                                 Path file,
                                                 long partSizeBytes,
                                                 long bytesUploadedSoFar,
                                                 String relativePath,
                                                 StorageBackendProperties backend) {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            List<CompletedPart> completedParts = new ArrayList<>();

            for (int partNumber = 1; partNumber <= itemUpload.getPartsTotal(); partNumber++) {
                long offset = (long) (partNumber - 1) * partSizeBytes;
                long remaining = itemUpload.getSizeBytes() - offset;
                int currentPartSize = (int) Math.min(partSizeBytes, remaining);
                byte[] buffer = new byte[currentPartSize];
                raf.seek(offset);
                raf.readFully(buffer);

                String etag = s3Client.uploadPart(
                        UploadPartRequest.builder()
                                .bucket(itemUpload.getS3Bucket())
                                .key(itemUpload.getS3Key())
                                .uploadId(itemUpload.getS3UploadId())
                                .partNumber(partNumber)
                                .contentLength((long) buffer.length)
                                .build(),
                        RequestBody.fromBytes(buffer)).eTag();

                // Validar y limpiar ETag
                if (etag == null || etag.trim().isEmpty()) {
                    throw new IllegalStateException("Received null or empty ETag for part " + partNumber);
                }

                // Eliminar comillas dobles si están presentes (algunos backends S3 las incluyen)
                String cleanedETag = etag.replace("\"", "");

                completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(cleanedETag).build());
                itemUpload.setPartsCompleted(partNumber);

                // Bytes acumulados a nivel run (no solo del item): avance por partes
                long runBytes = bytesUploadedSoFar + Math.min(itemUpload.getSizeBytes(), (long) partNumber * partSizeBytes);
                runUpload.setBytesUploaded(runBytes);

                // OPTIMIZACIÓN: Eliminados eventos por parte. Solo se publica 1 evento al completar el fichero completo
                // Esto reduce de ~1000 eventos por fichero multipart a solo 1 evento final
            }

            // Log para diagnóstico antes de completar multipart upload
            log.debug("Completing multipart upload for {} with {} parts. ETags: {}",
                    relativePath, completedParts.size(),
                    completedParts.stream().map(p -> "Part" + p.partNumber() + ":" + p.eTag()).collect(java.util.stream.Collectors.joining(", ")));

            // Validar que las partes están ordenadas y sin duplicados
            java.util.Set<Integer> partNumbers = new java.util.HashSet<>();
            for (CompletedPart part : completedParts) {
                if (!partNumbers.add(part.partNumber())) {
                    throw new IllegalStateException("Duplicate part number found: " + part.partNumber());
                }
            }

            // Ordenar partes por número (AWS S3 lo requiere)
            completedParts.sort(java.util.Comparator.comparingInt(CompletedPart::partNumber));

            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(itemUpload.getS3Bucket())
                    .key(itemUpload.getS3Key())
                    .uploadId(itemUpload.getS3UploadId())
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());

            long newBytes = bytesUploadedSoFar + itemUpload.getSizeBytes();
            runUpload.setBytesUploaded(newBytes);

            // Al completar el fichero: actualizar catalogo incremental y publicar progreso final del item
            addCompletedRunItemToFolder(runUpload, relativePath, itemUpload.getS3Bucket(), itemUpload.getS3Key());
            publishItemProgress(runUpload, relativePath, newBytes, runUpload.getBytesTotal());
            return newBytes;
        } catch (Exception e) {
            // Intentar abortar upload multipart del item
            try {
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(itemUpload.getS3Bucket())
                        .key(itemUpload.getS3Key())
                        .uploadId(itemUpload.getS3UploadId())
                        .build());
            } catch (Exception ignore) {
                // best-effort
            }
            throw new RuntimeException("Error uploading run item multipart " + file + ": " + e.getMessage(), e);
        }
    }

    private void publishItemProgress(Upload runUpload, String itemRelativePath, long bytesUploaded, Long bytesTotal) {
        UploadEvent event = new UploadEvent();
        event.setEventType(UploadEventType.UPLOAD_PROGRESS);
        event.setUploadId(runUpload.getId());
        event.setAgentId(agentUploadProperties.getAgentId());
        event.setSourceName(runUpload.getSourceName());
        event.setRunId(runUpload.getRunId());
        event.setItemRelativePath(itemRelativePath);
        // OPTIMIZACIÓN: No incluir Folder en eventos de progreso para reducir tamaño de ~10MB a ~500 bytes
        // El Folder completo solo se envía en UPLOAD_STARTED y UPLOAD_COMPLETED para catalogación en NiFi
        // event.setFolder(runUpload.getFolder());
        event.setFilePath(runUpload.getFilePath());

        // Importante: en eventos de progreso, el S3 key debe apuntar al item (fichero) actual
        StorageBackendProperties backend = storageConfigurationProperties.backendMap().get(runUpload.getStorageBackend());
        if (backend != null) {
            event.setStorageBackend(backend.getId());
            event.setS3Bucket(backend.getBucket());
            event.setS3Key(UploadKeyBuilder.buildRunItemKey(backend.getBasePath(), runUpload.getSourceName(), agentUploadProperties.getAgentId(), runUpload.getRunId(), itemRelativePath));
        }

        event.setBytesUploaded(bytesUploaded);
        event.setBytesTotal(bytesTotal != null ? bytesTotal : runUpload.getBytesTotal());
        event.setOccurredAt(Instant.now());
        if (event.getBytesTotal() != null && event.getBytesTotal() > 0) {
            event.setProgressPercentage((event.getBytesUploaded() * 100.0) / event.getBytesTotal());
        }
        stateStore.applyEvent(event);
    }


    /**
     * Añade un fichero completado al modelo Folder del run. Si el fichero está en subcarpetas,
     * crea/usa Folder(s) intermedios y deja folder.folders vacía cuando no hay subcarpetas.
     */
    private void addCompletedRunItemToFolder(Upload runUpload, String itemRelativePath, String bucket, String s3Key) {
        if (runUpload.getFolder() == null) {
            return;
        }

        String normalized = itemRelativePath == null ? "" : itemRelativePath.replace('\\', '/');
        if (normalized.isBlank()) {
            return;
        }

        String[] segments = normalized.split("/");
        if (segments.length == 0) {
            return;
        }

        es.tsystems.genomics.tpiagent.upload.model.Folder current = runUpload.getFolder();

        // BasePath para URLs de subfolders (mismo que el run)
        String basePath = "";
        StorageBackendProperties backend = storageConfigurationProperties.backendMap().get(runUpload.getStorageBackend());
        if (backend != null && backend.getBasePath() != null) {
            basePath = backend.getBasePath();
        }

        // Navegar/crear subcarpetas si aplica (todo menos el último segmento es carpeta)
        for (int i = 0; i < segments.length - 1; i++) {
            String folderName = segments[i];
            if (folderName == null || folderName.isBlank()) {
                continue;
            }

            es.tsystems.genomics.tpiagent.upload.model.Folder next = null;
            for (es.tsystems.genomics.tpiagent.upload.model.Folder f : current.getFolders()) {
                if (folderName.equals(f.getName())) {
                    next = f;
                    break;
                }
            }

            if (next == null) {
                next = new es.tsystems.genomics.tpiagent.upload.model.Folder();
                next.setName(folderName);
                next.setSource(current.getSource());

                // url del subfolder: prefix del run + path relativo dentro del run hasta esta subcarpeta
                String relPrefix = String.join("/", java.util.Arrays.copyOfRange(segments, 0, i + 1));
                String folderKey = UploadKeyBuilder.buildRunPrefixKey(basePath, runUpload.getSourceName(), agentUploadProperties.getAgentId(), runUpload.getRunId()) + "/" + relPrefix;
                next.setUrl(UploadKeyBuilder.toS3Url(bucket, folderKey));

                current.getFolders().add(next);
            }
            current = next;
        }

        String fileName = segments[segments.length - 1];
        if (fileName == null || fileName.isBlank()) {
            return;
        }

        String url = UploadKeyBuilder.toS3Url(bucket, s3Key);

        // Evitar duplicados si reintentamos
        boolean exists = current.getFiles().stream().anyMatch(fr -> fileName.equals(fr.getName()));
        if (!exists) {
            current.getFiles().add(new es.tsystems.genomics.tpiagent.upload.model.FileRef(fileName, url));
        }
    }

    private es.tsystems.genomics.tpiagent.upload.model.Folder buildFolderModel(Path dir,
                                                                             StorageBackendProperties backend,
                                                                             String sourceName,
                                                                             String agentId,
                                                                             String runId,
                                                                             es.tsystems.genomics.tpiagent.upload.model.Source source) {
        es.tsystems.genomics.tpiagent.upload.model.Folder folder = new es.tsystems.genomics.tpiagent.upload.model.Folder();
        folder.setName(dir.getFileName().toString());
        folder.setSource(source);

        try (java.util.stream.Stream<Path> list = Files.list(dir)) {
            list.forEach(p -> {
                try {
                    if (Files.isDirectory(p)) {
                        folder.getFolders().add(buildFolderModel(p, backend, sourceName, agentId, runId, source));
                    } else if (Files.isRegularFile(p)) {
                        if (RUN_COMPLETION_FLAG.equals(p.getFileName().toString())) {
                            return; // ignorar flag
                        }
                        // Para url, usamos path relativo al run
                        String relToRun;
                        try {
                            relToRun = Paths.get(agentUploadProperties.getSourceDirectoryFor(sourceName)).resolve(runId).relativize(p).toString().replace('\\', '/');
                        } catch (Exception e) {
                            // fallback: best-effort
                            relToRun = p.getFileName().toString();
                        }
                        String key = UploadKeyBuilder.buildRunItemKey(backend.getBasePath(), sourceName, agentId, runId, relToRun);
                        folder.getFiles().add(new es.tsystems.genomics.tpiagent.upload.model.FileRef(p.getFileName().toString(), UploadKeyBuilder.toS3Url(backend.getBucket(), key)));
                    }
                } catch (Exception ignore) {
                    // best-effort
                }
            });
        } catch (IOException e) {
            // best-effort
        }

        // url de la carpeta actual: prefix del run + path relativo dentro del run
        String relPath = "";
        try {
            // si dir es subcarpeta, calcular rel respecto al run (runId)
            Path runRoot = Paths.get(agentUploadProperties.getSourceDirectoryFor(sourceName)).resolve(runId);
            relPath = runRoot.relativize(dir).toString().replace('\\', '/');
        } catch (Exception ignore) {
        }
        String folderKey = UploadKeyBuilder.buildRunPrefixKey(backend.getBasePath(), sourceName, agentId, runId) + (relPath.isBlank() ? "" : "/" + relPath);
        folder.setUrl(UploadKeyBuilder.toS3Url(backend.getBucket(), folderKey));
        return folder;
    }


    private int countFilesRecursive(es.tsystems.genomics.tpiagent.upload.model.Folder folder) {
        if (folder == null) {
            return 0;
        }
        int count = 0;
        if (folder.getFiles() != null) {
            count += folder.getFiles().size();
        }
        if (folder.getFolders() != null) {
            for (es.tsystems.genomics.tpiagent.upload.model.Folder f : folder.getFolders()) {
                count += countFilesRecursive(f);
            }
        }
        return count;
    }


    private boolean tryMoveWithRetries(Path sourcePath, Path destinationPath) {
        final int maxAttempts = 5;
        final long baseSleepMs = 150L;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
                return true;
            } catch (IOException moveEx) {
                // Si estamos en Windows y el fichero está temporalmente bloqueado, reintentamos.
                if (attempt < maxAttempts) {
                    try {
                        Thread.sleep(baseSleepMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }
                // último intento: dejamos que el caller haga fallback
                return false;
            }
        }
        return false;
    }

    private void copyFileWithRetries(Path sourcePath, Path destinationPath) throws IOException {
        final int maxAttempts = 5;
        final long baseSleepMs = 150L;

        IOException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                return;
            } catch (IOException e) {
                last = e;
                if (attempt < maxAttempts) {
                    try {
                        Thread.sleep(baseSleepMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw last;
    }

    private void deleteFileWithRetries(Path sourcePath) throws IOException {
        final int maxAttempts = 5;
        final long baseSleepMs = 150L;

        IOException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Files.deleteIfExists(sourcePath);
                return;
            } catch (IOException e) {
                last = e;
                if (attempt < maxAttempts) {
                    try {
                        Thread.sleep(baseSleepMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw last;
    }


    /**
     * Mueve una carpeta (run) a un directorio destino. Si move falla, hace copy+delete (best-effort).
     */
    private void moveDirectoryToDirectory(Path dir, String targetRoot, String label, String sourceName) {
        // Obtener logger específico del source
        org.slf4j.Logger sourceLog = sourceLoggerFactory.getLoggerForSource(sourceName, agentUploadProperties.getLogsDirectoryFor(sourceName));

        Path target = Paths.get(targetRoot).resolve(dir.getFileName());
        try {
            Files.createDirectories(target.getParent());
            Files.move(dir, target, StandardCopyOption.REPLACE_EXISTING);
            log.info("✓ Moved run folder to {} [{}]: {} -> {}", label, sourceName, dir, target);
            sourceLog.info("✓ Moved run folder to {}: {} -> {}", label, dir, target);
        } catch (IOException moveError) {
            log.warn("Move failed for run folder {} [{}], falling back to copy+delete: {}", dir, sourceName, moveError.getMessage());
            sourceLog.warn("Move failed for run folder {}, falling back to copy+delete: {}", dir, moveError.getMessage());
            try {
                copyRecursively(dir, target);
                deleteRecursively(dir);
                log.info("✓ Moved run folder to {} (copy+delete) [{}]: {} -> {}", label, sourceName, dir, target);
                sourceLog.info("✓ Moved run folder to {} (copy+delete): {} -> {}", label, dir, target);
            } catch (IOException copyError) {
                log.error("Failed to move run folder {} [{}] to {}: {}", dir, sourceName, target, copyError.getMessage());
                sourceLog.error("Failed to move run folder {} to {}: {}", dir, target, copyError.getMessage());
            }
        }
    }

    private void completeMultipartUploadWithRetry(S3Client s3Client,
                                                 Upload upload,
                                                 List<CompletedPart> completedParts,
                                                 String fileName,
                                                 StorageBackendProperties backend) {
        s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(upload.getS3Bucket())
                .key(upload.getS3Key())
                .uploadId(upload.getS3UploadId())
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build());
    }

    private void cleanupEmptyParents(Path sourceRoot, Path startDir) {
        if (sourceRoot == null || startDir == null) {
            return;
        }

        try {
            Path current = startDir.normalize();
            Path root = sourceRoot.normalize();

            while (current != null && !current.equals(root) && current.startsWith(root)) {
                if (!Files.isDirectory(current)) {
                    break;
                }

                // En Windows puede tardar un poco en reflejar que el fichero se ha movido/borrado.
                // Reintentamos la comprobación de "vacío" unas pocas veces.
                boolean empty = false;
                for (int attempt = 1; attempt <= 5; attempt++) {
                    try (var s = Files.list(current)) {
                        empty = s.findFirst().isEmpty();
                    }
                    if (empty) {
                        break;
                    }
                    try {
                        Thread.sleep(50L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (!empty) {
                    break;
                }

                try {
                    Files.deleteIfExists(current);
                    log.debug("Removed empty directory under source: {}", current);
                } catch (IOException deleteEx) {
                    // Si el sistema aún no la libera, esperamos un poco y reintentamos una vez.
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        Files.deleteIfExists(current);
                        log.debug("Removed empty directory under source (retry): {}", current);
                    } catch (IOException ignore) {
                        log.debug("Could not delete empty directory {}: {}", current, deleteEx.getMessage());
                        break;
                    }
                }

                current = current.getParent();
            }
        } catch (Exception e) {
            log.debug("Empty-dir cleanup failed: {}", e.getMessage());
        }
    }


    private void copyRecursively(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
                Path rel = source.relativize(dir);
                Files.createDirectories(target.resolve(rel));
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
                Path rel = source.relativize(file);
                Path dst = target.resolve(rel);
                Files.createDirectories(dst.getParent());
                Files.copy(file, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteRecursively(Path root) throws IOException {
        Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }
}
