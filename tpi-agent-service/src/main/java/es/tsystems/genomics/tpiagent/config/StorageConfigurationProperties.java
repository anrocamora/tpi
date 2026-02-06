package es.tsystems.genomics.tpiagent.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Validated
@ConfigurationProperties(prefix = "storage")
public class StorageConfigurationProperties {

    @Valid
    @NotEmpty
    private List<StorageBackendProperties> backends;

    public List<StorageBackendProperties> getBackends() {
        return backends;
    }

    public void setBackends(List<StorageBackendProperties> backends) {
        this.backends = backends;
    }

    public Map<String, StorageBackendProperties> backendMap() {
        return backends.stream().collect(Collectors.toMap(StorageBackendProperties::getId, b -> b));
    }
}


