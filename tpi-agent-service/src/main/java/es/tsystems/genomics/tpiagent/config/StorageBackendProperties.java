package es.tsystems.genomics.tpiagent.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class StorageBackendProperties {
    @NotBlank
    private String id;

    @NotNull
    private StorageBackendType type;

    @NotBlank
    private String bucket;

    @NotBlank
    private String basePath;

    @NotBlank
    private String region;

    @NotBlank
    private String endpoint;

    private boolean pathStyleAccess;

    // HTTP timeout configuration (in seconds)
    @Min(1)
    private int connectionTimeoutSeconds = 60;

    // Socket read timeout - increased to 30 minutes (1800s) for Dell PowerScale CompleteMultipartUpload
    // Dell PowerScale can take a very long time to consolidate multipart uploads
    @Min(1)
    private int readTimeoutSeconds = 1800;

    // API call timeout - increased to 30 minutes (1800s) for Dell PowerScale CompleteMultipartUpload
    // This controls the total time allowed for S3 operations including server-side processing
    @Min(1)
    private int apiCallTimeoutSeconds = 1800;

    // Dell PowerScale workaround: use PutObject instead of multipart upload
    // Set to true for backends that have broken completeMultipartUpload
    private boolean useSinglePartUpload = false;

    // Post-consolidation validation settings for Dell PowerScale
    // These control how long we wait for the object to become available after CompleteMultipartUpload
    // NOTE: Dell ECS usually consolidates within seconds, not minutes
    // The initial wait before polling gives Dell ECS time to make the object available
    @Min(1)
    private int consolidationMaxRetries = 30;        // More retries but shorter delays

    @Min(1)
    private int consolidationInitialDelaySeconds = 1;  // Start with 1 second

    @Min(1)
    private int consolidationMaxDelaySeconds = 5;      // Max 5 seconds between retries

    @Min(1)
    private int consolidationMaxWaitSeconds = 60;      // Max 60 seconds total wait

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public StorageBackendType getType() {
        return type;
    }

    public void setType(StorageBackendType type) {
        this.type = type;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    public void setPathStyleAccess(boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }

    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    public void setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
    }

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    public int getApiCallTimeoutSeconds() {
        return apiCallTimeoutSeconds;
    }

    public void setApiCallTimeoutSeconds(int apiCallTimeoutSeconds) {
        this.apiCallTimeoutSeconds = apiCallTimeoutSeconds;
    }

    public boolean isUseSinglePartUpload() {
        return useSinglePartUpload;
    }

    public void setUseSinglePartUpload(boolean useSinglePartUpload) {
        this.useSinglePartUpload = useSinglePartUpload;
    }

    public int getConsolidationMaxRetries() {
        return consolidationMaxRetries;
    }

    public void setConsolidationMaxRetries(int consolidationMaxRetries) {
        this.consolidationMaxRetries = consolidationMaxRetries;
    }

    public int getConsolidationInitialDelaySeconds() {
        return consolidationInitialDelaySeconds;
    }

    public void setConsolidationInitialDelaySeconds(int consolidationInitialDelaySeconds) {
        this.consolidationInitialDelaySeconds = consolidationInitialDelaySeconds;
    }

    public int getConsolidationMaxDelaySeconds() {
        return consolidationMaxDelaySeconds;
    }

    public void setConsolidationMaxDelaySeconds(int consolidationMaxDelaySeconds) {
        this.consolidationMaxDelaySeconds = consolidationMaxDelaySeconds;
    }

    public int getConsolidationMaxWaitSeconds() {
        return consolidationMaxWaitSeconds;
    }

    public void setConsolidationMaxWaitSeconds(int consolidationMaxWaitSeconds) {
        this.consolidationMaxWaitSeconds = consolidationMaxWaitSeconds;
    }
}


