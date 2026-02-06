package es.tsystems.genomics.tpiagent.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class S3ClientFactoryTest {

    private S3ClientFactory factory;
    private StorageConfigurationProperties storageProperties;

    @BeforeEach
    void setUp() {
        StorageBackendProperties backend = new StorageBackendProperties();
        backend.setId("test-backend");
        backend.setType(StorageBackendType.AWS);
        backend.setBucket("test-bucket");
        backend.setBasePath("/test");
        backend.setRegion("us-east-1");
        backend.setEndpoint("http://localhost:9000");
        backend.setPathStyleAccess(true);

        List<StorageBackendProperties> backends = new ArrayList<>();
        backends.add(backend);

        storageProperties = new StorageConfigurationProperties();
        storageProperties.setBackends(backends);

        factory = new S3ClientFactory(storageProperties);
    }

    @Test
    void testClientForReturnsS3Client() {
        S3Client client = factory.clientFor("test-backend");
        assertNotNull(client, "S3Client should not be null");
    }

    @Test
    void testClientForCachesClient() {
        S3Client client1 = factory.clientFor("test-backend");
        S3Client client2 = factory.clientFor("test-backend");
        assertSame(client1, client2, "Should return cached client instance");
    }

    @Test
    void testClientForUnknownBackendThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> factory.clientFor("unknown-backend")
        );
        assertTrue(exception.getMessage().contains("Unknown storage backend"));
    }

    @Test
    void testConstructorWithNullStoragePropertiesThrowsException() {
        assertThrows(NullPointerException.class, () -> new S3ClientFactory(null));
    }
}



