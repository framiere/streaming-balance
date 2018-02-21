package com.github.framiere.balance;

import lombok.*;

import java.math.BigDecimal;
import java.util.Date;


public class Domain {
	@Data
	@EqualsAndHashCode(of = "accountId")
	@NoArgsConstructor
	@AllArgsConstructor
	@Builder
	public static class Account {
		public int id;
		public String name;
		public Market market;
		public BigDecimal balance;
	}

	enum Market {
		NASDAQ,
		NYSE
	}


	@Data
	@EqualsAndHashCode(of = "operationId")
	@NoArgsConstructor
	@AllArgsConstructor
	@Builder
	public static class Operation {
		public int accountId;
		public Market market;
		public String stock;
		public OperationType operationType;
		public Double value;
		public Date instant;
	}

	public enum OperationType {
		BUY,
		SELL,
		COMPENSATION
	}
}
