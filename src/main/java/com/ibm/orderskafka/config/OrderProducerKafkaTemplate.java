package com.ibm.orderskafka.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderProducerKafkaTemplate {

	@Autowired
	private KafkaConfiguration kafkaConfiguration;

	@Bean("orderProducerKafkaTemplateA") 
	public KafkaProducer<String, String> getOrderProducerKafkaTemplate() {
		Properties properties = kafkaConfiguration.buildProducerProperties();
		return new KafkaProducer<String, String>(properties);
	}
}
