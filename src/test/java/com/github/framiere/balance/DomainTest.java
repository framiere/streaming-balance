package com.github.framiere.balance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.framiere.balance.Domain.Account;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;


public class DomainTest {
	private final Account framiere = new Account(1, "framiere", Domain.Market.NASDAQ, BigDecimal.valueOf(1000));

	@Test
	public void shouldSerializedInJson() throws JsonProcessingException {
		assertThat(new ObjectMapper().writeValueAsString(framiere))
				.isEqualTo("{\"id\":1,\"name\":\"framiere\"}");
	}

	@Test
	public void shouldDeserializedInJson() throws IOException {
		Account account = new ObjectMapper().readValue("{\"id\":1,\"name\":\"framiere\"}", Account.class);
		assertThat(account)
				.isEqualToComparingFieldByField(framiere);
	}
}
