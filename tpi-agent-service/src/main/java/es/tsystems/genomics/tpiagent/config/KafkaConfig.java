package es.tsystems.genomics.tpiagent.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import es.tsystems.genomics.tpiagent.upload.model.UploadStateSnapshot;
import es.tsystems.genomics.tpiagent.upload.service.UploadEvent;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${agent.upload.agent-id}")
    private String agentId;

    @Value("${spring.kafka.properties.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.ssl.truststore.location:}")
    private String truststoreLocation;

    @Value("${spring.kafka.properties.ssl.truststore.password:}")
    private String truststorePassword;

    @Value("${spring.kafka.properties.ssl.truststore.type:JKS}")
    private String truststoreType;

    @Value("${spring.kafka.properties.ssl.keystore.location:}")
    private String keystoreLocation;

    @Value("${spring.kafka.properties.ssl.keystore.password:}")
    private String keystorePassword;

    @Value("${spring.kafka.properties.ssl.keystore.type:JKS}")
    private String keystoreType;

    @Value("${spring.kafka.properties.ssl.key.password:}")
    private String keyPassword;

    @Value("${spring.kafka.properties.ssl.endpoint.identification.algorithm:https}")
    private String endpointIdentificationAlgorithm;

    @Value("${spring.kafka.producer.properties.max.request.size:20971520}")
    private int maxRequestSize;

    @Value("${spring.kafka.producer.properties.buffer.memory:104857600}")
    private long bufferMemory;

    @Value("${spring.kafka.consumer.properties.max.partition.fetch.bytes:20971520}")
    private int maxPartitionFetchBytes;

    @Value("${spring.kafka.consumer.properties.fetch.max.bytes:52428800}")
    private int fetchMaxBytes;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = JacksonUtils.enhancedObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    /**
     * Configura las propiedades comunes para productores de Kafka (SSL, seguridad, mensajes grandes)
     */
    private Map<String, Object> getCommonProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Configuración para mensajes grandes
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);

        // Configuración de idempotencia y fiabilidad
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // Configuración de timeouts ampliados para mensajes grandes
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000); // 2 minutos
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 240000); // 4 minutos
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120000); // 2 minutos

        // Add SSL configuration if security protocol is SSL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        if ("SSL".equals(securityProtocol)) {
            if (!truststoreLocation.isEmpty()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreType);
            }
            if (!keystoreLocation.isEmpty()) {
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType);
                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
            }
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentificationAlgorithm);
        }

        return props;
    }

    /**
     * Producer para eventos de upload (UploadEvent)
     */
    @Bean
    public ProducerFactory<String, UploadEvent> uploadEventProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = getCommonProducerProps();
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, UploadEvent> kafkaTemplate(ProducerFactory<String, UploadEvent> uploadEventProducerFactory) {
        return new KafkaTemplate<>(uploadEventProducerFactory);
    }

    /**
     * Producer para snapshots de estado (UploadStateSnapshot - lightweight DTO)
     */
    @Bean
    public ProducerFactory<String, UploadStateSnapshot> uploadStateProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = getCommonProducerProps();
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, UploadStateSnapshot> stateSnapshotTemplate(ProducerFactory<String, UploadStateSnapshot> uploadStateProducerFactory) {
        return new KafkaTemplate<>(uploadStateProducerFactory);
    }

    /**
     * Configura las propiedades comunes para consumidores de Kafka
     */
    private Map<String, Object> getCommonConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // Configuración para mensajes grandes
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);

        // Configuración de timeouts ampliados
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000); // 2 minutos
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 90000); // 1.5 minutos
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 30000); // 30 segundos

        // Add SSL configuration if security protocol is SSL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        if ("SSL".equals(securityProtocol)) {
            if (!truststoreLocation.isEmpty()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreType);
            }
            if (!keystoreLocation.isEmpty()) {
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType);
                props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
            }
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentificationAlgorithm);
        }

        return props;
    }

    @Bean
    public ConsumerFactory<String, UploadStateSnapshot> stateRecoveryConsumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = getCommonConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, agentId + "-state-recovery");

        JsonDeserializer<UploadStateSnapshot> valueDeserializer = new JsonDeserializer<>(UploadStateSnapshot.class, objectMapper);
        valueDeserializer.trustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UploadStateSnapshot> stateRecoveryListenerFactory(
            ConsumerFactory<String, UploadStateSnapshot> stateRecoveryConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, UploadStateSnapshot> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stateRecoveryConsumerFactory);
        return factory;
    }
}


