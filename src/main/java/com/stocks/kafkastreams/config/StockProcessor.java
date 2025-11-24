package com.stocks.kafkastreams.config;

import com.stocks.kafkastreams.dto.AnomalyAlert;
import com.stocks.kafkastreams.dto.StockPrice;
import com.stocks.kafkastreams.utils.DoubleArraySerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;

@Configuration
@EnableKafkaStreams
@Slf4j
public class StockProcessor {

    @Value("${app.topic.input-prices}")
    private String inputTopic;

    @Value("${app.topic.output-ticker}")
    private String tickerTopic;

    @Value("${app.topic.output-anomaly}")
    private String anomalyTopic;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {

        JsonSerde<StockPrice> stockPriceSerde = new JsonSerde<>(StockPrice.class);

        //Store the historical max for anomaly checks
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("anomaly-state-store"),
                        Serdes.String(),
                        Serdes.Double()
                )
        );

        KStream<String, StockPrice> input =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), stockPriceSerde));

        //We need two branches of the stream because the ticker topology modifies the key
        //and makes it unusable in the anomaly topology
        KStream<String, StockPrice> tickerStream = input.mapValues(v -> v);
        KStream<String, StockPrice> anomalyStream = input.mapValues(v -> v);

        buildTickerTopology(tickerStream);
        buildAnomalyTopology(anomalyStream, stockPriceSerde);
    }

    /**
     * Configuration for a topology that every 2 seconds produces an average of stock prices within a 10-second window
     * and publishes them to a kafka topic
     *
     * @param stream
     */
    private void buildTickerTopology(KStream<String, StockPrice> stream) {
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))
                .advanceBy(Duration.ofSeconds(2));

        stream.mapValues(StockPrice::getPrice)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(hoppingWindow)
                .aggregate(
                        () -> new double[]{0.0, 0.0},
                        (key, price, agg) -> {
                            agg[0] += price;
                            agg[1] += 1;
                            return agg;
                        },
                        Materialized.<String, double[], WindowStore<Bytes, byte[]>>as("ticker-store") //temp store for the values
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new DoubleArraySerde())
                )
                .toStream()
                .mapValues(agg -> agg[0] / agg[1])
                .selectKey((windowedKey, value) -> windowedKey.key())
                .to(tickerTopic, Produced.with(Serdes.String(), Serdes.Double()));
    }

    private void buildAnomalyTopology(KStream<String, StockPrice> stream, JsonSerde<StockPrice> valSerde) {

        //Processor API is used to compare the previous max price before updating
        stream
                .processValues(AnomalyDetectorProcessor::new, Named.as("anomaly-detector"), "anomaly-state-store")
                .filter((key, alert) -> alert != null)
                .to(anomalyTopic, Produced.with(
                        Serdes.String(),
                        new JsonSerde<>(AnomalyAlert.class)
                ));
    }

    /**
     * Custom Processor that detects price anomalies by comparing current price
     * against the previous historical maximum
     */
    static class AnomalyDetectorProcessor implements org.apache.kafka.streams.processor.api.FixedKeyProcessor<String, StockPrice, AnomalyAlert> {

        private org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<String, AnomalyAlert> context;
        private org.apache.kafka.streams.state.KeyValueStore<String, Double> stateStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(org.apache.kafka.streams.processor.api.FixedKeyProcessorContext<String, AnomalyAlert> context) {
            this.context = context;
            this.stateStore = context.getStateStore("anomaly-state-store");
        }

        @Override
        public void process(org.apache.kafka.streams.processor.api.FixedKeyRecord<String, StockPrice> record) {
            String symbol = record.key();
            StockPrice currentStock = record.value();
            double currentPrice = currentStock.getPrice();

            //Get previous max
            Double previousMax = stateStore.get(symbol);

            AnomalyAlert alert = null;

            if (previousMax != null) {
                double threshold = previousMax * 1.05;

                log.debug("Symbol: {}, Current: {}, PreviousMax: {}, Threshold: {}",
                        symbol, currentPrice, previousMax, threshold);

                if (currentPrice > threshold) {
                    double deviation = currentPrice - previousMax;
                    alert = new AnomalyAlert(
                            symbol,
                            currentPrice,
                            previousMax,
                            deviation,
                            String.format("Price spike detected: %s jumped %.2f%% from $%.2f to $%.2f",
                                    symbol,
                                    (deviation / previousMax) * 100,
                                    previousMax,
                                    currentPrice),
                            Instant.now()
                    );
                    log.info("🚨 ANOMALY DETECTED: {}", alert.getAlertMessage());
                }
            }

            // Update state store with new max
            if (previousMax == null || currentPrice > previousMax) {
                stateStore.put(symbol, currentPrice);
                log.debug("Updated max price for {}: {}", symbol, currentPrice);
            }

            context.forward(record.withValue(alert));
        }

        @Override
        public void close() {
            // cleanup is managed by streams
        }
    }
}