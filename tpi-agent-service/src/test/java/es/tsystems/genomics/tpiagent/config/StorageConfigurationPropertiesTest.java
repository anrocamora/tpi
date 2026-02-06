package es.tsystems.genomics.tpiagent.config;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StorageConfigurationPropertiesTest {

    @Test
    void testBackendMapCreatesCorrectMapping() {
        StorageBackendProperties backend1 = new StorageBackendProperties();
        backend1.setId("backend-1");
        backend1.setType(StorageBackendType.AWS);
        backend1.setBucket("bucket-1");
        backend1.setBasePath("/path1");
        backend1.setRegion("us-east-1");
        backend1.setEndpoint("http://endpoint1");

        StorageBackendProperties backend2 = new StorageBackendProperties();
        backend2.setId("backend-2");
        backend2.setType(StorageBackendType.AWS);
        backend2.setBucket("bucket-2");
        backend2.setBasePath("/path2");
        backend2.setRegion("us-west-2");
        backend2.setEndpoint("http://endpoint2");

        List<StorageBackendProperties> backends = new ArrayList<>();
        backends.add(backend1);
        backends.add(backend2);

        StorageConfigurationProperties config = new StorageConfigurationProperties();
        config.setBackends(backends);

        Map<String, StorageBackendProperties> map = config.backendMap();

        assertEquals(2, map.size());
        assertSame(backend1, map.get("backend-1"));
        assertSame(backend2, map.get("backend-2"));
    }

    @Test
    void testGettersAndSetters() {
        List<StorageBackendProperties> backends = new ArrayList<>();
        StorageConfigurationProperties config = new StorageConfigurationProperties();
        config.setBackends(backends);

        assertSame(backends, config.getBackends());
    }
}



