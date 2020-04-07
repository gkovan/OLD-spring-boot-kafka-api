package com.ibm.orderskafka.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderConsumerKafkaTemplate {
	
	@Autowired
	private KafkaConfiguration kafkaConfiguration;

	@Bean 
	public KafkaConsumer<String, String> getOrderConsumerKafkaTemplate() {
    	// if we need to have multiple threads then the clientId needs to be different
    	// auto commit is set to true, and read from the last not committed offset
    	Properties properties = kafkaConfiguration.buildConsumerProperties();
		return new KafkaConsumer<String, String>(properties);
	}
}
