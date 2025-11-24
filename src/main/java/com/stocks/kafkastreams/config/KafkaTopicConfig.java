package com.stocks.kafkastreams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // Inject topic names from application.yaml
    @Value("${app.topic.input-prices}")
    private String inputPricesTopic;

    @Value("${app.topic.output-ticker}")
    private String outputTickerTopic;

    @Value("${app.topic.output-anomaly}")
    private String outputAnomalyTopic;

    //Producer sends data here
    @Bean
    public NewTopic inputPricesTopic() {
        return TopicBuilder.name(inputPricesTopic)
                .partitions(3) // Recommend more partitions for key-based processing
                .replicas(3)
                .build();
    }

    //From input topic produce average price results in this
    @Bean
    public NewTopic outputTickerTopic() {
        return TopicBuilder.name(outputTickerTopic)
                .partitions(3)
                .replicas(3)
                .build();
    }

    //From input topic produce price anomaly records in this
    @Bean
    public NewTopic outputAnomalyTopic() {
        return TopicBuilder.name(outputAnomalyTopic)
                .partitions(1)
                .replicas(3)
                .build();
    }

    //For monitoring the max price table
    @Bean
    public NewTopic maxPriceReadableTopic() {
        return TopicBuilder.name("max-price-readable")
                .partitions(1)
                .replicas(3)
                .build();
    }
}
