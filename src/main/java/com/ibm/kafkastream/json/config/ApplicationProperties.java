package com.ibm.kafkastream.json.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
//@ConfigurationProperties(prefix = "kafka.json")
public class ApplicationProperties {

	
    public Map<String, Object> getProducerConfigs(String bootstrapServers, String apikey) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);       
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-java-console-sample-producer");
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG,"use_all_dns_ips");
        configs.putAll(getCommonConfigs(bootstrapServers, apikey));
        return configs;
    }
    
    public Map<String, Object> getStreamConfigs(String bootstrapServers, String apikey) {
    	Map<String, Object> configs = new HashMap<>();
		configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-json");
    	configs.putAll(getCommonConfigs(bootstrapServers, apikey));
    	return configs;
    }
    
    public Map<String, Object> getCommonConfigs(String boostrapServers, String apikey) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"token\" password=\"" + apikey + "\";");
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2");
        configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        return configs;
    }
}
