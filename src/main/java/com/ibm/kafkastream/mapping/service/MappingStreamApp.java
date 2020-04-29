package com.ibm.kafkastream.mapping.service;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.wordcount.service.WordCountKafkaStreamOperator;

@Service
public class MappingStreamApp {

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountKafkaStreamOperator.class);

	private KafkaStreams streams = null;

    public MappingStreamApp() {
		super();
        final Properties props = getStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

	}

	public static final String INPUT_TOPIC = "mapping-stream-input";
    public static final String OUTPUT_TOPIC = "mapping-stream-output";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mapping-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        return props;
    }

    /** This stream mapping key to value and value to key.
     *
     * @param builder
     */
    static void createStream(StreamsBuilder builder) {
        final Serde<String> stringSerde = new Serdes.StringSerde();
        final KStream<String, String> source = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));
        final KStream<String, String> mapped = source.map((key, value) -> new KeyValue<String,String>(value, key));
        source.groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
        .count()
        .toStream()
        .map((Windowed<String> key, Long count) -> new KeyValue(key.key(), count.toString()))
        .to(OUTPUT_TOPIC,Produced.with(stringSerde, stringSerde));
        		
        //mapped.to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
    }
    
    static void createStreamKeyIsLongValueIsString(StreamsBuilder builder) {
        final Serde<String> stringSerde = new Serdes.StringSerde();
        final Serde<Long> longSerde = new Serdes.LongSerde();
        final KStream<Long, String> source = builder.stream(INPUT_TOPIC, Consumed.with(longSerde, stringSerde));
        final KStream<String, Long> mapped = source.map((key, value) -> new KeyValue<String,Long>(value, key));
        mapped.to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
    }

	public void start() {
		LOGGER.info("Mapping Stream Service started.");
		try {
			streams.start();
		} catch (final Throwable e) {
			LOGGER.error("Error starting the Mapping Stream Service.", e);
		}
	}

	public void stop() {
		LOGGER.info("Mapping Stream Service stopped");
		streams.close();
	}


}
