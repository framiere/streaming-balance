package com.github.framiere.balance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.github.framiere.balance.Domain.Operation;

public class Stream {

	public void stream(String bootstrapServers) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "balance2");
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 00);
		properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer, Operation> input = builder.stream("operation", Consumed.with(Serdes.Integer(), new JsonSerde(Operation.class)));

		// map each value and add the thread that processed it
		input
				.mapValues(v -> Thread.currentThread().getName() + " " + v)
				.to("operation-with-thread", Produced.with(Serdes.Integer(), Serdes.String()));


		// ./kafka-console-consumer \
		// 			--bootstrap-server localhost:9092 \
		// 			--topic balance-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog \
		// 			--property print.key=true \
		// 			--property key.separator="-" \
		// 			--key-deserializer org.apache.kafka.common.serialization.IntegerDeserializer \
		// 			--value-deserializer org.apache.kafka.common.serialization.LongDeserializer
		input
				.groupByKey(Serialized.with(Serdes.Integer(), new JsonSerde(Operation.class)))
				.count();

		// ./kafka-console-consumer \
		// 			--bootstrap-server localhost:9092 \
		// 			--topic balance-number-of-operations-per-account-changelog \
		// 			--property print.key=true \
		// 			--property key.separator="-" \
		// 			--key-deserializer org.apache.kafka.common.serialization.IntegerDeserializer \
		// 			--value-deserializer org.apache.kafka.common.serialization.LongDeserializer
		input
				.groupByKey(Serialized.with(Serdes.Integer(), new JsonSerde(Operation.class)))
				.count(Materialized.<Integer, Long, StateStore>as("number-of-operations-per-account")
						.withKeySerde(Serdes.Integer())
						.withValueSerde(Serdes.Long()));

		// ./kafka-console-consumer \
		// 			--bootstrap-server localhost:9092 \
		// 			--topic number-of-operations-per-account-on-5s-window-above-0 \
		// 			--property print.key=true \
		// 			--property key.separator="-" \
		// 			--key-deserializer org.apache.kafka.streams.kstream.internals.WindowedDeserializer \
		// 			--value-deserializer org.apache.kafka.common.serialization.LongDeserializer
		input
				.groupByKey(Serialized.with(Serdes.Integer(), new JsonSerde(Operation.class)))
				.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(5)))
				.count(Materialized.<Integer, Long, StateStore>as("number-of-operations-per-account-on-5s-window")
						.withKeySerde(Serdes.Integer())
						.withValueSerde(Serdes.Long()))
				.toStream((windowedRegion, count) -> windowedRegion.toString())
				.filter((windowedRegion, count) -> (Long) count >= 1)
				.to("number-of-operations-per-account-on-5s-window-above-0", Produced.with(Serdes.String(), Serdes.Long()));


		// ./kafka-console-consumer \
		// 			--bootstrap-server localhost:9092 \
		// 			--topic aggregate-of-operation-over-5-second-windows \
		// 			--property print.key=true \
		// 			--property key.separator="-" \
		// 			--key-deserializer org.apache.kafka.common.serialization.StringDeserializer
		// 			--value-deserializer org.apache.kafka.common.serialization.DoubleDeserializer
		input
				.groupByKey(Serialized.with(Serdes.Integer(), new JsonSerde(Operation.class)))
				.windowedBy(SessionWindows.with(TimeUnit.SECONDS.toMillis(5)))
				.aggregate(
						new Initializer<Double>() {
							@Override
							public Double apply() {
								return 0d;
							}
						},
						new Aggregator<Integer, Operation, Double>() {
							@Override
							public Double apply(Integer key, Operation value, Double aggregate) {
								return aggregate + value.value;
							}
						},
						new Merger<Integer, Double>() {
							@Override
							public Double apply(Integer aggKey, Double aggOne, Double aggTwo) {
								return (aggOne + aggTwo) / 2;
							}
						},
						Materialized.<Integer, Double, StateStore>as("aggregate-of-operation-over-5-second-windows")
								.withKeySerde(Serdes.Integer())
								.withValueSerde(Serdes.Double())
				)
				.toStream((windowedRegion, count) -> windowedRegion.toString())
				.peek((k, v) -> System.out.println(k + " " + v));


		input
				.process(() -> new AbstractProcessor<Integer, Operation>() {
					private final List<Operation> batch = new ArrayList<>();

					@Override
					public void init(ProcessorContext context) {
						super.init(context);
						// Punctuator function will be called on the same thread
						context().schedule(TimeUnit.SECONDS.toMillis(10), PunctuationType.WALL_CLOCK_TIME, this::flush);
					}

					private void flush(long timestamp) {
						if (!batch.isEmpty()) {
							// sending to an external system ?
							System.out.println(timestamp + " " + Thread.currentThread().getName() + " Flushing batch of " + batch.size());
							batch.clear();
						}
					}

					@Override
					public void process(Integer key, Operation operation) {
						batch.add(operation);
						context().forward(key, operation);
					}
				});

		Topology build = builder.build();

		System.out.println(build.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
	}

	public static void main(String[] args) {
		String bootstrapServers = args.length == 1 ? args[0] : "localhost:9092";
		System.out.println(bootstrapServers);
		new Stream().stream(bootstrapServers);
	}
}
