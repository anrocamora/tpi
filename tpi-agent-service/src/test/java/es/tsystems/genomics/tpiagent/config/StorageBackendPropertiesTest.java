package es.tsystems.genomics.tpiagent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StorageBackendPropertiesTest {

    @Test
    void testGettersAndSetters() {
        StorageBackendProperties backend = new StorageBackendProperties();

        backend.setId("backend-1");
        backend.setType(StorageBackendType.AWS);
        backend.setBucket("my-bucket");
        backend.setBasePath("/uploads");
        backend.setRegion("us-west-2");
        backend.setEndpoint("https://s3.amazonaws.com");
        backend.setPathStyleAccess(false);

        assertEquals("backend-1", backend.getId());
        assertEquals(StorageBackendType.AWS, backend.getType());
        assertEquals("my-bucket", backend.getBucket());
        assertEquals("/uploads", backend.getBasePath());
        assertEquals("us-west-2", backend.getRegion());
        assertEquals("https://s3.amazonaws.com", backend.getEndpoint());
        assertFalse(backend.isPathStyleAccess());
    }

    @Test
    void testPathStyleAccessDefaultValue() {
        StorageBackendProperties backend = new StorageBackendProperties();
        assertFalse(backend.isPathStyleAccess(), "Default pathStyleAccess should be false");
    }
}



