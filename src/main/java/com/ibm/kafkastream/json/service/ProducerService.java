package com.ibm.kafkastream.json.service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.json.config.ApplicationProperties;
import com.ibm.kafkastream.json.config.JsonPOJOSerializer;
import com.ibm.kafkastream.json.model.MyRequest;


@Service
public class ProducerService {
	
	
	private Producer<String, MyRequest> producer;
	
	
	public ProducerService() {
		super();
		
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", JsonPOJOSerializer.class);

		 producer = new KafkaProducer<>(props);

	}

	private Logger LOGGER = LoggerFactory.getLogger(this.getClass().getName());

	
	private String TOPIC_NAME = "json-input";
	
	public void sendRecord(MyRequest myRequest) {
		ProducerRecord<String, MyRequest> producerRecord = new ProducerRecord<>(TOPIC_NAME, myRequest);
		
		Future<RecordMetadata> send = producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("##### Error sending record to Kafka topic");
            } else {
                LOGGER.info("##### Successfully sent to topic. Offset is {}", metadata.offset());
            }
        });
		try {
			 send.get(10L, TimeUnit.SECONDS);

		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// TODO Auto-generated catch block
			LOGGER.error("Error producing record to kafka topic:", e);
		}
	}

}
