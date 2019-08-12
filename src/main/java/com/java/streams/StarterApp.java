package com.java.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

public class StarterApp {

	public static void main(String[] args) {
		Properties configuration = new Properties();
		configuration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-demo2");
		configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		//disable cache in dev
		configuration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		StreamsBuilder builder = new StreamsBuilder();

		builder.stream("topic3",Consumed.with(Serdes.String(), Serdes.String()))
				.filter((x,y)-> y.contains(","))
				.peek((x,y)->System.out.println("key is "+ x+", value is "+y))
				.map(new KeyValueMapper<String, String, KeyValue<String,String>>() {

					@Override
					public KeyValue<String, String> apply(String key, String value) {
						String[] arr=value.split(",");
						return new KeyValue<String, String>(arr[0], arr[1]);
					}
				})
				.peek((x,y)->System.out.println("key is "+ x+", value is "+y)).to("temp");
				KTable<String, Long> table=builder.table("temp", Consumed.with(Serdes.String(),Serdes.String()))
				.groupBy(new KeyValueMapper<String, String, KeyValue<String,String>>() {

					@Override
					public KeyValue<String, String> apply(String key, String value) {
						return new KeyValue<String, String>(value, value);
					}
				})
				.count();

		table.toStream()
			.to("topic4", Produced.with(Serdes.String(), Serdes.Long()));
		KafkaStreams s = new KafkaStreams(builder.build(), configuration);
		//cleanUp: only to be done in dev.
		s.cleanUp();
		s.start();// starts the application
		System.out.println("Stream is" + s.toString());
		Runtime.getRuntime().addShutdownHook(new Thread(s::close));

	}
}
