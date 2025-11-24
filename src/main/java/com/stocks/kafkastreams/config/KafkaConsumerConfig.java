package com.stocks.kafkastreams.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configuration class for custom Kafka Consumer settings, specifically handling deserialization failures.
 * This prevents the application from getting stuck on corrupted messages in a topic.
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Creates a custom Kafka Listener Container Factory with a robust Error Handler.
     * @param consumerFactory The standard Spring Boot consumer factory configured in application.properties.
     * @return The custom ConcurrentKafkaListenerContainerFactory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);


        // Set max reties at 3 after which error is logged and record is skipped
        FixedBackOff fixedBackOff = new FixedBackOff(0L, 3L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}