package com.ibm.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@ConfigurationProperties(prefix = "kafka")
public class CommonKafkaPropertiesConfiguration {

    public CommonKafkaPropertiesConfiguration() {}

    private final Map<String, String> commonProperties = new HashMap<>();
    public Map<String, String> getCommonProperties(){
        return this.commonProperties;
    }

    public Properties buildCommonProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, commonProperties.get("bootstrapServers"));

        if (!commonProperties.get("apiKey").isEmpty()) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, commonProperties.get("securityProtocol"));
            properties.put(SaslConfigs.SASL_MECHANISM, commonProperties.get("saslMechanism"));
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            commonProperties.get("username"),
                            commonProperties.get("apiKey")));
            properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, commonProperties.get("sslProtocol"));
            properties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, commonProperties.get("sslEnabledProtocols"));
            properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, commonProperties.get("sslEndpointIdentificationAlgorithm"));

            if ("true".equals(commonProperties.get("truststoreEnabled"))){
                properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, commonProperties.get("sslTruststoreLocation"));
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, commonProperties.get("sslTruststorePassword"));
            }
        }
        return properties;
    }
}
