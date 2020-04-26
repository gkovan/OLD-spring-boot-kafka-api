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

@Service
public class PipeStreamService {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipeStreamService.class);

	private KafkaStreams streams = null;
	private Topology topology = null;
	private final StreamsBuilder builder = new StreamsBuilder();
	private final Properties props = new Properties();
	
	public static String INPUT_TOPIC = "streams-pipe-input";
	public static String OUTPUT_TOPIC = "streams-pipe-output";
	
	public Topology getTopology() {
		return topology;
	}
	
	public Properties getProperties() {
		return props;
	}

	public PipeStreamService() {
		super();
		// TODO Auto-generated constructor stub

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);
		
        topology = builder.build();
        
        streams = new KafkaStreams(topology, props);
	}

	public void start() {
		LOGGER.info("Pipe Stream Service started.");
		try {
			streams.start();
		} catch (final Throwable e) {
			LOGGER.error("Error starting the Pipe Stream Service.", e);
		}
	}

	public void stop() {
		LOGGER.info("Pipe Stream Service stopped");
		streams.close();
	}

}
