package com.stocks.kafkastreams.consumer;

import com.stocks.kafkastreams.dto.AnomalyAlert;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class AnomalyConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnomalyConsumer.class);

    @KafkaListener(
            topics = "${app.topic.output-anomaly}",
            groupId = "anomaly-detector-group",
            properties = {
                    "spring.json.value.default.type=com.stocks.kafkastreams.dto.AnomalyAlert"
            }
    )
    public void listen(AnomalyAlert alert) {
        log.warn("🔥 ANOMALY DETECTED for Ticker: {}", alert.getSymbol());
        log.warn("   Price: {}, Mean: {}, StdDev: {}, Message: {}",
                alert.getCurrentPrice(),
                alert.getMeanPrice(),
                alert.getStandardDeviation(),
                alert.getAlertMessage());
    }

}
