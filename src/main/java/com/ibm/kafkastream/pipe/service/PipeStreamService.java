package com.ibm.kafkastream.pipe.service;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.wordcount.service.WordCountKafkaStreamOperator;

@Service
public class PipeStreamService {

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountKafkaStreamOperator.class);

	private KafkaStreams streams = null;
	private Topology topology = null;
	private final StreamsBuilder builder = new StreamsBuilder();
	private final Properties props = new Properties();

	public PipeStreamService() {
		super();
		// TODO Auto-generated constructor stub

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		builder.stream("streams-pipe-input").to("streams-pipe-output");
		
        topology = builder.build();
        
        streams = new KafkaStreams(topology, props);
	}

	public void start() {
		LOGGER.info("WordCount Stream Operator started.");
		try {
			streams.start();
		} catch (final Throwable e) {
			LOGGER.error("Error starting the WordCount Stream Operator.", e);
		}
	}

	public void stop() {
		LOGGER.info("WordCount Stream Operator stopped");
		streams.close();
	}

}
