package es.tsystems.genomics.tpiagent.upload.service;

import es.tsystems.genomics.tpiagent.config.AgentUploadProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DirectoryUploadSchedulerTest {

    @TempDir
    Path tempDir;

    private DirectoryUploadScheduler scheduler;
    private AgentUploadProperties properties;
    private UploadService uploadService;
    private UploadStateStore stateStore;
    private SourceLoggerFactory sourceLoggerFactory;

    private Path inbox;
    private Path testSourceDir; // inbox/TestSource/agent-1/source

    @BeforeEach
    void setUp() throws IOException {
        inbox = tempDir.resolve("inbox");
        Files.createDirectories(inbox);

        properties = new AgentUploadProperties();
        properties.setAgentId("agent-1");
        properties.setInboxDirectory(inbox.toString());
        properties.setMoveDirectoryDatasets(false);
        properties.setScanIntervalMs(30000);
        properties.setFileStabilityWindowMs(0); // para tests, estable inmediatamente si mtime no cambia

        // Crear estructura para un source de prueba: inbox/TestSource/agent-1/source
        testSourceDir = inbox.resolve("TestSource").resolve("agent-1").resolve("source");
        Files.createDirectories(testSourceDir);
        Files.createDirectories(inbox.resolve("TestSource").resolve("agent-1").resolve("completed"));
        Files.createDirectories(inbox.resolve("TestSource").resolve("agent-1").resolve("failed"));
        Files.createDirectories(inbox.resolve("TestSource").resolve("agent-1").resolve("logs"));

        uploadService = mock(UploadService.class);
        stateStore = mock(UploadStateStore.class);
        sourceLoggerFactory = mock(SourceLoggerFactory.class);
        when(stateStore.hasActiveUploadForPath(anyString())).thenReturn(false);
        when(sourceLoggerFactory.getLoggerForSource(anyString(), anyString())).thenReturn(org.slf4j.LoggerFactory.getLogger("test"));

        scheduler = new DirectoryUploadScheduler(properties, uploadService, stateStore, sourceLoggerFactory);
        scheduler.setRecoveryCompleted(true);
    }

    @Test
    void testStagesFileFromInboxRootToSourceAndUploads() throws IOException {
        // Archivo en inbox/TestSource/file1.txt debe moverse a inbox/TestSource/agent-1/source/file1.txt
        Path testSource = inbox.resolve("TestSource");
        Files.createDirectories(testSource);
        Path file1 = testSource.resolve("file1.txt");
        Files.writeString(file1, "content1");

        scheduler.scanAndUpload();

        Path staged = testSourceDir.resolve("file1.txt");
        assertTrue(Files.exists(staged), "file should be moved into source");
        assertFalse(Files.exists(file1), "original inbox file should be moved");

        // En el nuevo modo por run, un fichero suelto en source no dispara uploadRunFolder.
        verify(uploadService, never()).uploadRunFolder(any(), any());
    }

    @Test
    void testStagesFileFromInboxSubfolderPreservingStructure() throws IOException {
        Path testSource = inbox.resolve("TestSource");
        Path sub = testSource.resolve("run01");
        Files.createDirectories(sub);

        Path file = sub.resolve("reads.fastq.gz");
        Files.writeString(file, "data");

        scheduler.scanAndUpload();

        Path staged = testSourceDir.resolve("run01").resolve("reads.fastq.gz");
        assertTrue(Files.exists(staged));
        assertFalse(Files.exists(file));

        // Sin flag, no debe iniciar upload del run
        verify(uploadService, never()).uploadRunFolder(any(), any());
    }

    @Test
    void testScanSourceUploadsExistingRunsWhenFlagPresent() throws IOException {
        Path run = testSourceDir.resolve("runX");
        Files.createDirectories(run);
        Files.writeString(run.resolve("a.txt"), "a");
        Files.writeString(run.resolve("RunCompletionStatus.xml"), "<ok/>");

        scheduler.scanAndUpload();

        verify(uploadService, times(1)).uploadRunFolder(eq(run), eq("TestSource"));
    }

    @Test
    void testScanSourceDoesNotUploadRunWhenFlagMissing() throws IOException {
        Path run = testSourceDir.resolve("runNoFlag");
        Files.createDirectories(run);
        Files.writeString(run.resolve("a.txt"), "a");

        scheduler.scanAndUpload();

        verify(uploadService, never()).uploadRunFolder(any(), any());
    }

    @Test
    void testMovesDirectoryDatasetFromInboxToSourceWhenStable() throws IOException {
        // Simular modo PRE: datasets en inbox/TestSource/run_001 -> inbox/TestSource/agent-1/source/run_001
        properties.setMoveDirectoryDatasets(true);
        properties.setFileStabilityWindowMs(0);

        // Re-instanciar para evitar estado/capturas anteriores
        scheduler = new DirectoryUploadScheduler(properties, uploadService, stateStore, sourceLoggerFactory);
        scheduler.setRecoveryCompleted(true);

        // Dataset externo en inbox/TestSource/run_001 (no dentro de la estructura del agente)
        Path testSource = inbox.resolve("TestSource");
        Path dataset = testSource.resolve("run_001");
        Files.createDirectories(dataset);
        Path f1 = dataset.resolve("a.txt");
        Path f2 = dataset.resolve("nested").resolve("b.txt");
        Files.createDirectories(f2.getParent());
        Files.writeString(f1, "a");
        Files.writeString(f2, "b");
        // Flag requerido
        Files.writeString(dataset.resolve("RunCompletionStatus.xml"), "<ok/>");

        scheduler.scanAndUpload();

        Path stagedDir = testSourceDir.resolve("run_001");
        assertTrue(Files.exists(stagedDir), "dataset directory should be moved into agent source");
        assertFalse(Files.exists(dataset), "original dataset directory should be moved away");

        // Se debería subir el run con sourceName
        verify(uploadService, times(1)).uploadRunFolder(eq(stagedDir), eq("TestSource"));
    }

    @Test
    void testIgnoresAgentRootUnderInbox() throws IOException {
        // En el nuevo modelo, los archivos dentro de inbox/TestSource/agent-1/... no deben re-moverse
        properties.setMoveDirectoryDatasets(true);
        properties.setFileStabilityWindowMs(0);

        scheduler = new DirectoryUploadScheduler(properties, uploadService, stateStore, sourceLoggerFactory);
        scheduler.setRecoveryCompleted(true);

        // Un fichero ya dentro de la estructura del agente no debe re-moverse
        Path alreadyInAgent = inbox.resolve("TestSource").resolve("agent-1").resolve("somewhere.txt");
        Files.writeString(alreadyInAgent, "x");

        scheduler.scanAndUpload();

        // No esperamos uploads porque el fichero no está en source/runX con flag
        verify(uploadService, never()).uploadRunFolder(any(), any());
    }

    @Test
    void testMovesDirectoryDatasetInDevWhenInboxIsAgentRoot() throws IOException {
        // Caso DEV/local: inbox contiene sources en primer nivel
        // inbox/MiSeq/run -> inbox/MiSeq/agent-1/source/run
        properties.setMoveDirectoryDatasets(true);
        properties.setFileStabilityWindowMs(0);

        scheduler = new DirectoryUploadScheduler(properties, uploadService, stateStore, sourceLoggerFactory);
        scheduler.setRecoveryCompleted(true);

        // Crear un source MiSeq y un dataset dentro
        Path miseqSource = inbox.resolve("MiSeq");
        Files.createDirectories(miseqSource);

        // Crear estructura para MiSeq
        Path miseqAgentSource = miseqSource.resolve("agent-1").resolve("source");
        Files.createDirectories(miseqAgentSource);
        Files.createDirectories(miseqSource.resolve("agent-1").resolve("completed"));
        Files.createDirectories(miseqSource.resolve("agent-1").resolve("failed"));
        Files.createDirectories(miseqSource.resolve("agent-1").resolve("logs"));

        Path dataset = miseqSource.resolve("M05089_155_000000000-CT8YM");
        Files.createDirectories(dataset);
        Files.writeString(dataset.resolve("a.txt"), "a");
        Files.createDirectories(dataset.resolve("nested"));
        Files.writeString(dataset.resolve("nested").resolve("b.txt"), "b");
        Files.writeString(dataset.resolve("RunCompletionStatus.xml"), "<ok/>");

        scheduler.scanAndUpload();

        Path stagedDir = miseqAgentSource.resolve("M05089_155_000000000-CT8YM");
        assertTrue(Files.exists(stagedDir), "dataset directory should be moved into source");
        assertFalse(Files.exists(dataset), "original dataset directory should be moved away");

        verify(uploadService, times(1)).uploadRunFolder(eq(stagedDir), eq("MiSeq"));
    }
}
