package com.ibm.kafkastream.wordcount.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WordCountProducerKafkaTemplate {

	@Autowired
	private WordCountKafkaConfiguration kafkaConfiguration;

	@Bean("wordCountProducerKafkaTemplateA")
	public KafkaProducer<String, String> getWordCountProducerKafkaTemplate() {
		Properties properties = kafkaConfiguration.buildProducerProperties();
		return new KafkaProducer<String, String>(properties);
	}
}
