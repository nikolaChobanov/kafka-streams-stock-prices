package com.stocks.kafkastreams.producer;

import com.stocks.kafkastreams.dto.StockPrice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Random;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockProducer {

    private final KafkaTemplate<String, StockPrice> kafkaTemplate;

    @Value("${app.topic.input-prices}")
    private String inputTopic;

    private final Random random = new Random();
    private final String[] symbols = {"GOOG", "MSFT", "AMZN", "AAPL"};
    private final double[] basePrices = {150.00, 450.00, 180.00, 220.00};

    @Scheduled(fixedRate = 500)
    public void generateStockPrice() {
        int index = random.nextInt(symbols.length);
        String symbol = symbols[index];
        double basePrice = basePrices[index];

        // Normal fluctuation
        double fluctuation = (random.nextDouble() - 0.5) * 5;
        double newPrice = basePrice + fluctuation;

        // Spike in value for anomaly
        if (random.nextInt(50) == 0) {
            log.info("⚡ INJECTING PRICE SPIKE for {}", symbol);
            newPrice *= 1.15; // 15% increase (guaranteed to trigger the 5% threshold)
        }

        StockPrice stockPrice = new StockPrice(symbol, newPrice, Instant.now());

        // 3. Send to Kafka (Key = Symbol, Value = JSON Object)
        kafkaTemplate.send(inputTopic, symbol, stockPrice);
    }
}
