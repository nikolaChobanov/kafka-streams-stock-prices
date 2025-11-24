package com.stocks.kafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkastreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkastreamsApplication.class, args);
	}

}
