package com.ibm.kafkastream.window.service;

import org.springframework.stereotype.Service;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Service
public class WindowStreamService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WindowStreamService.class);

	private KafkaStreams streams = null;

    public WindowStreamService() {
		super();
        final Properties props = getStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

	}

	public static final String INPUT_TOPIC = "window-stream-input";
    public static final String OUTPUT_TOPIC = "window-stream-output";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "window-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        return props;
    }

    /** Counts the number of times the input record 'key' occurs in the specified kafka window duration.
     *
     * @param builder
     */
    static void createStream(StreamsBuilder builder) {
        final Serde<String> stringSerde = new Serdes.StringSerde();
        final KStream<String, String> source = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));
        final KStream<String, String> mapped = source.map((key, value) -> new KeyValue<String,String>(key, value));
        source.map((key, record) -> new KeyValue<String,String>(key,record))
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
        .count()
        .toStream()
        .map((Windowed<String> key, Long count) -> new KeyValue(key.key(), count.toString()))
        .to(OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));        		
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
