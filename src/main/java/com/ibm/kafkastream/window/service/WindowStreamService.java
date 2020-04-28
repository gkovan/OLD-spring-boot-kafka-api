package com.ibm.kafkastream.window.service;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.pipe.service.PipeStreamService;

@Service
public class WindowStreamService {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipeStreamService.class);

	private KafkaStreams streams = null;
	private Topology topology = null;
	private final StreamsBuilder builder = new StreamsBuilder();
	private final Properties props = new Properties();
	
	public static String INPUT_TOPIC = "window-stream-input";
	public static String OUTPUT_TOPIC = "window-stream-output";

	public WindowStreamService() {
		super();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		//KStream<String, String> source = builder.stream(INPUT_TOPIC);
		//source.to(OUTPUT_TOPIC);
		builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);		
        topology = builder.build();
        
        streams = new KafkaStreams(topology, props);
	}
	
	public void start() {
		LOGGER.info("Window Stream Service started.");
		try {
			streams.start();
		} catch (final Throwable e) {
			LOGGER.error("Error starting the Window Stream Service.", e);
		}
	}

	public void stop() {
		LOGGER.info("Window Stream Service stopped");
		streams.close();
	}
}
