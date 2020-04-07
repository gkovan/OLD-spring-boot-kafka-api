package com.ibm.orderskafka.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.config.CommonKafkaPropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka.orders")
public class KafkaConfiguration {
	private static final Logger logger = Logger.getLogger(KafkaConfiguration.class.getName());
	public static final long PRODUCER_TIMEOUT_SECS = 10;
	public static final long PRODUCER_CLOSE_TIMEOUT_SEC = 10;
	public static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(10);
    public static final long TERMINATION_TIMEOUT_SEC = 10;
    private Properties commonProperties;

	private final Map<String, String> sharedProperties = new HashMap<>();
	public Map<String, String> getSharedProperties(){
		return this.sharedProperties;
	}

	private final Map<String, String> producerProperties = new HashMap<>();
	public Map<String, String> getProducerProperties(){
		return this.producerProperties;
	}

	private final Map<String, String> consumerProperties = new HashMap<>();
	public Map<String, String> getConsumerProperties(){
		return this.consumerProperties;
	}

	@Autowired
    public KafkaConfiguration(CommonKafkaPropertiesConfiguration commonKafkaPropertiesConfiguration) {
		this.commonProperties = commonKafkaPropertiesConfiguration.buildCommonProperties();
	}
    
    public Properties buildConsumerProperties() {
		Properties properties = new Properties();
		properties.putAll(commonProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,  consumerProperties.get("groupId"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerProperties.get("enableAutoCommit"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerProperties.get("autoOffsetReset"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerProperties.get("clientId"));
        properties.forEach((k,v)  -> logger.info(k + " : " + v)); 
        return properties;
    }


	public String getOrdersTopicName() {
		return sharedProperties.get("topic");
	}

	public Properties buildProducerProperties() {
		Properties properties = new Properties();
		properties.putAll(commonProperties);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerProperties.get("clientId"));
		properties.put(ProducerConfig.ACKS_CONFIG, producerProperties.get("acks"));
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerProperties.get("enableIdempotence"));
		properties.forEach((k,v)  -> logger.info(k + " : " + v));
		return properties;
	}

}
