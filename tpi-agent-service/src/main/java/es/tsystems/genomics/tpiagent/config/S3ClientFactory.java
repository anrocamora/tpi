package es.tsystems.genomics.tpiagent.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class S3ClientFactory {
    private static final Logger log = LoggerFactory.getLogger(S3ClientFactory.class);

    private final StorageConfigurationProperties storageProperties;
    private final Map<String, S3Client> s3Clients = new ConcurrentHashMap<>();

    public S3ClientFactory(StorageConfigurationProperties storageProperties) {
        this.storageProperties = Objects.requireNonNull(storageProperties);
    }

    public S3Client clientFor(String backendId) {
        return s3Clients.computeIfAbsent(backendId, this::buildClient);
    }


    private S3Client buildClient(String backendId) {
        StorageBackendProperties backend = storageProperties.backendMap().get(backendId);
        if (backend == null) {
            throw new IllegalArgumentException("Unknown storage backend: " + backendId);
        }

        // Configure HTTP client with custom timeouts
        SdkHttpClient httpClient = ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(backend.getConnectionTimeoutSeconds()))
                .socketTimeout(Duration.ofSeconds(backend.getReadTimeoutSeconds()))
                .build();

        // Configure client override with API call timeout
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(backend.getApiCallTimeoutSeconds()))
                .build();

        // Configure S3 client for compatibility with non-AWS S3 servers (e.g., Dell ECS, MinIO)
        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(backend.isPathStyleAccess())
                .checksumValidationEnabled(false)  // Disable checksum validation for compatibility
                .build();

        log.info("📡 Configuring S3 client for backend '{}' [{}] with timeouts: connection={}s, read={}s, apiCall={}s",
                backend.getId(), backend.getType(), backend.getConnectionTimeoutSeconds(),
                backend.getReadTimeoutSeconds(), backend.getApiCallTimeoutSeconds());

        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(backend.getRegion()))
                .endpointOverride(URI.create(backend.getEndpoint()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClient(httpClient)
                .overrideConfiguration(overrideConfig)
                .serviceConfiguration(s3Config);

        return builder.build();
    }
}

