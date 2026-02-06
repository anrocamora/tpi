package es.tsystems.genomics.tpiagent.upload.service;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class UploadKeyBuilderTest {

    @Test
    void buildKeyNormalizesBasePathSlash() {
        assertEquals("base/MiSeq/agent-1/a/b.txt", UploadKeyBuilder.buildKey("base", "MiSeq", "agent-1", "a/b.txt"));
        assertEquals("base/MiSeq/agent-1/a/b.txt", UploadKeyBuilder.buildKey("base/", "MiSeq", "agent-1", "a/b.txt"));
    }

    @Test
    void buildRunPrefixAndItemKey() {
        assertEquals("base/MiSeq/agent-1/run01", UploadKeyBuilder.buildRunPrefixKey("base", "MiSeq", "agent-1", "run01"));
        assertEquals("base/MiSeq/agent-1/run01/reads.fastq.gz",
                UploadKeyBuilder.buildRunItemKey("base", "MiSeq", "agent-1", "run01", "reads.fastq.gz"));
    }

    @Test
    void toS3Url() {
        assertEquals("s3://bucket/base/key", UploadKeyBuilder.toS3Url("bucket", "base/key"));
    }

    @Test
    void relativizeToSourceRootUsesForwardSlashes() {
        String rel = UploadKeyBuilder.relativizeToSourceRoot("C:/data/source", Path.of("C:/data/source/run01/reads.fastq.gz"));
        assertEquals("run01/reads.fastq.gz", rel);
    }

    @Test
    void relativizeToSourceRootFallsBackToFileName() {
        String rel = UploadKeyBuilder.relativizeToSourceRoot("C:/data/source", Path.of("D:/other/reads.fastq.gz"));
        assertEquals("reads.fastq.gz", rel);
    }
}

