package com.ibm.kafkastream.wordcount.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.ibm.config.CommonKafkaPropertiesConfiguration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka.word-count")
public class WordCountKafkaConfiguration {
	private static final Logger logger = Logger.getLogger(WordCountKafkaConfiguration.class.getName());
	public static final long PRODUCER_TIMEOUT_SECS = 10;
	public static final long PRODUCER_CLOSE_TIMEOUT_SEC = 10;
	public static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(10);
    public static final long TERMINATION_TIMEOUT_SEC = 10;

    private Properties commonProperties;

	private final Map<String, String> producerProperties = new HashMap<>();
	public Map<String, String> getProducerProperties(){
		return this.producerProperties;
	}

    private final Map<String, String> streamProperties = new HashMap<>();
    public Map<String, String> getStreamProperties(){
        return this.streamProperties;
    }

    @Autowired
    public WordCountKafkaConfiguration(CommonKafkaPropertiesConfiguration commonKafkaPropertiesConfiguration) {
        this.commonProperties = commonKafkaPropertiesConfiguration.buildCommonProperties();
    }

	public String getWordCountProducerTopicName() {
	    return producerProperties.get("topic");
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

	public Properties buildStreamingProperties(){
        Properties properties = new Properties();
        properties.putAll(commonProperties);

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, streamProperties.get("applicationId"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }

    public String getStreamingInputTopicName() {
        return streamProperties.get("inputTopic");
    }

    public String getStreamingOutputTopicName() {
        return streamProperties.get("outputTopic");
    }

    public String getStreamingStoreName() {
        return streamProperties.get("store-name");
    }
}
