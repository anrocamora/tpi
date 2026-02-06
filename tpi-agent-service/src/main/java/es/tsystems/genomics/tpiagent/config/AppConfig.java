package es.tsystems.genomics.tpiagent.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({StorageConfigurationProperties.class, AgentUploadProperties.class})
public class AppConfig {
}

