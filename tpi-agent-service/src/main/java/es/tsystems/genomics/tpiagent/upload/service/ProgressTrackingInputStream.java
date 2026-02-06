package es.tsystems.genomics.tpiagent.upload.service;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * InputStream wrapper that tracks bytes read and reports progress via callback.
 * Used for monitoring real-time upload progress during single-part S3 uploads.
 */
public class ProgressTrackingInputStream extends FilterInputStream {
    private final long totalBytes;
    private final Consumer<Long> progressCallback;
    private long bytesRead = 0;

    /**
     * Create a progress-tracking input stream.
     *
     * @param in The underlying input stream to read from
     * @param totalBytes Total expected bytes (for progress calculation)
     * @param progressCallback Callback invoked with cumulative bytes read
     */
    public ProgressTrackingInputStream(InputStream in, long totalBytes, Consumer<Long> progressCallback) {
        super(in);
        this.totalBytes = totalBytes;
        this.progressCallback = progressCallback;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b != -1) {
            bytesRead++;
            progressCallback.accept(bytesRead);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int count = super.read(b, off, len);
        if (count > 0) {
            bytesRead += count;
            progressCallback.accept(bytesRead);
        }
        return count;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public double getProgressPercentage() {
        if (totalBytes == 0) return 0.0;
        return (bytesRead * 100.0) / totalBytes;
    }
}

