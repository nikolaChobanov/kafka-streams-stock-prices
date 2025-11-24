package com.stocks.kafkastreams.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StockListener {

    @KafkaListener(
            topics = "${app.topic.output-ticker}",
            groupId = "ticker-consumer",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer"
            }
    )
    public void handleTicker(Double average) {
        log.info("📈 Ticker Update: Moving Avg = {}", String.format("%.2f", average));
    }
}