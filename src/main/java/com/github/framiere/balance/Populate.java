package com.github.framiere.balance;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.github.framiere.balance.Domain.*;
import static java.util.stream.Collectors.toList;

/**
 * ./confluent destroy
 * ./confluent start kafka
 * ./kafka-topics --zookeeper localhost:2181 --create --partitions 16 --replication-factor 1 --topic account
 * ./kafka-topics --zookeeper localhost:2181 --create --partitions 16 --replication-factor 1 --topic operation
 */
public class Populate {
	private static final Faker faker = new Faker();
	public static final BigDecimal MINUS_ONE = BigDecimal.valueOf(-1);

	public static void main(String args[]) throws InterruptedException {
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
		Producer<Object, Object> producer = new KafkaProducer<>(props);


		List<Account> accounts = IntStream.range(1, 4000)
				.mapToObj(i -> Account.builder()
						.id(i)
						.name(faker.name().name())
						.market(randomMarket())
						.balance(BigDecimal.valueOf(faker.number().numberBetween(1, 100000)))
						.build())
				.peek(System.out::println)
				.peek(account -> producer.send(new ProducerRecord("account", account.id, account)))
				.collect(toList());


		while (true) {
			Account account = randomAccount(accounts);
			Market market = randomMarket();
			OperationType operationType = operationType();
			Operation operation = Operation.builder()
					.accountId(account.id)
					.market(market)
					.stock(randomStock(market))
					.operationType(operationType)
					.value(randomOperationValue(operationType))
					.instant(new Date())
					.build();
			producer.send(new ProducerRecord("operation", operation.accountId, operation));
			System.out.println(operation);
			TimeUnit.MILLISECONDS.sleep(40);
		}
	}

	private static double randomOperationValue(OperationType operationType) {
		double value = random.nextInt(1_000_000);
		switch (operationType) {
			case BUY:
				return value;
			case SELL:
				return -value;
			case COMPENSATION:
				return (random.nextInt(2) % 2 == 0) ? value : -value;
			default:
				throw new RuntimeException(operationType + " is not supported operation type");

		}
	}

	private static OperationType operationType() {
		return randomOperationType();
	}

	private static Market randomMarket() {
		return randomEnum(Market.class);
	}

	private static OperationType randomOperationType() {
		return randomEnum(OperationType.class);
	}

	private static final SecureRandom random = new SecureRandom();

	private static <T extends Enum<?>> T randomEnum(Class<T> clazz) {
		int x = random.nextInt(clazz.getEnumConstants().length);
		return clazz.getEnumConstants()[x];
	}

	private static Account randomAccount(List<Account> accounts) {
		return accounts.get(random.nextInt(accounts.size()));
	}

	private static String randomStock(Market market) {
		switch (market) {
			case NASDAQ:
				return faker.stock().nsdqSymbol();
			case NYSE:
				return faker.stock().nyseSymbol();
			default:
				throw new RuntimeException(market + " is not supported market");
		}
	}

}
