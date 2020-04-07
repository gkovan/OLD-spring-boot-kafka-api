package com.ibm.kafkastream.wordcount.service;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import com.ibm.kafkastream.wordcount.config.WordCountKafkaConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WordCountKafkaStreamOperator {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountKafkaStreamOperator.class);

    private final KafkaStreams streams;
    private final Topology topology;
    private final WordCountKafkaConfiguration kafkaConfiguration;
    final StreamsBuilder builder = new StreamsBuilder();
    Properties props = new Properties();


    @Autowired
    public WordCountKafkaStreamOperator(WordCountKafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
        defineStream();
        topology = builder.build();
        streams = new KafkaStreams(topology, props);
        LOGGER.info("WordCountKafkaStreamOperator has been initialized and is running");
    }
    
    private void defineStream() {
        props = kafkaConfiguration.buildStreamingProperties();
    	        
        KStream<String, String> source = builder.stream(kafkaConfiguration.getStreamingInputTopicName());
       
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
              .groupBy((key, value) -> value)
              .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(kafkaConfiguration.getStreamingStoreName()))
              .toStream()
              .to(kafkaConfiguration.getStreamingOutputTopicName(), Produced.with(Serdes.String(), Serdes.Long()));
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
