package com.ibm.kafkastream.json.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.json.config.ApplicationProperties;
import com.ibm.kafkastream.json.config.JsonPOJODeserializer;
import com.ibm.kafkastream.json.config.JsonPOJOSerializer;
import com.ibm.kafkastream.json.model.MyRequest;


/*
 * Example using JSON serializer/deserializer
 * https://github.com/apache/kafka/blob/1.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
 */

@Service
public class StreamService {



	private static final Logger LOGGER = LoggerFactory.getLogger(StreamService.class);

	private KafkaStreams streams = null;
	private Topology topology = null;
	private final StreamsBuilder builder = new StreamsBuilder();
	private final Properties props = new Properties();
	
	public static String INPUT_TOPIC = "json-input";
	public static String OUTPUT_TOPIC = "json-input";
	
	final Serde<String> stringSerde = new Serdes.StringSerde();


//	ApplicationProperties appProps = new ApplicationProperties();
	
	@Autowired
	public StreamService(ApplicationProperties appProps) {
		super();
		// TODO Auto-generated constructor stub
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-json");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJODeserializer.class);
		
		Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<MyRequest> myRequestSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", MyRequest.class);
        myRequestSerializer.configure(serdeProps, false);

        final Deserializer<MyRequest> myRequestDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", MyRequest.class);
        myRequestDeserializer.configure(serdeProps, false);
		
		final Serde<MyRequest> myRequestSerde = Serdes.serdeFrom(myRequestSerializer, myRequestDeserializer);

		
		KStream<String, MyRequest> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), myRequestSerde));
		
		KStream<String, MyRequest> output = source.mapValues((record) -> {
			LOGGER.info("############# Request name: {}", record.getRequestName());
			record.setRequestName(record.getRequestName().toUpperCase());
			return record;
		}).filter((key, record) -> myfilterRecord(record));;
		output.to(OUTPUT_TOPIC, Produced.with(stringSerde, myRequestSerde));
		
        topology = builder.build();
        
//        streams = new KafkaStreams(topology, props);
        
		 String bootstrapServers = "broker-5-cll14zkm8222msbg.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-4-cll14zkm8222msbg.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093";
		 String apikey = "LMiMktfiLQZX5hYpeph53nCjuH8b7kU0QKg1PDB5KufV";
		 Map<String, Object> myMaps = appProps.getStreamConfigs(bootstrapServers, apikey);
		 Properties myProps = new Properties();
		 myProps.putAll(myMaps);
         streams = new KafkaStreams(topology, myProps);

	}
	
	private boolean myfilterRecord(MyRequest record) {
		// TODO Auto-generated method stub
		return record.getRequestName().contains("ramesh");
	}

	public Topology getTopology() {
		return topology;
	}
	
	public Properties getProperties() {
		return props;
	}
	
	public void start() {
		
		try {
			streams.start();
			LOGGER.info("Stream Service started.");
		} catch (final Throwable e) {
			LOGGER.error("Error starting the Stream Service.", e);
		}
	}

	public void stop() {
		streams.close();
		LOGGER.info("Stream Service stopped");
	}
}
