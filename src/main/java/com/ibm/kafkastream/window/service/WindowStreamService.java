package com.ibm.kafkastream.window.service;

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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


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
        
        // this parameter sets the commit interval.  
        // for this example, we count the keys on the input record
        // the count is stored in a Ktable.
        // this param controls how often the data is commited to the Ktable.
        // without the suppress, an output record is emitted when a commit is done
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        
        return props;
    }

    /** Counts the number of times the input record 'key' occurs in the specified kafka window duration.
     *
     * @param builder
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	static void createStream(StreamsBuilder builder) {
        final Serde<String> stringSerde = new Serdes.StringSerde();
        final KStream<String, String> source = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));
        final KStream<String, String> mapped = source.map((key, value) -> new KeyValue<String,String>(key, value));
        source.map((key, record) -> new KeyValue<String,String>(key,record))
        .groupByKey()
        // without grace, the default is 24 hours so window will not close for a while
        .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).grace(Duration.ofSeconds(2))) 
        //.count()
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        // suppress output records until the window closes
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
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
