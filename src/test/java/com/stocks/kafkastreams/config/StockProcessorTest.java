package com.stocks.kafkastreams.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stocks.kafkastreams.dto.AnomalyAlert;
import com.stocks.kafkastreams.dto.StockPrice;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Kafka Streams topologies using TopologyTestDriver.
 * Tests both ticker and anomaly detection topologies.
 */
class StockProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, StockPrice> inputTopic;
    private TestOutputTopic<String, Double> tickerOutputTopic;
    private TestOutputTopic<String, AnomalyAlert> anomalyOutputTopic;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static final String INPUT_TOPIC = "stock-prices-input";
    private static final String TICKER_TOPIC = "stock-ticker-output";
    private static final String ANOMALY_TOPIC = "stock-anomaly-alerts";

    @BeforeEach
    void setUp() {
        // Configure test properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stock-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build topology using a mock StockProcessor
        StreamsBuilder builder = new StreamsBuilder();
        TestStockProcessor processor = new TestStockProcessor(INPUT_TOPIC, TICKER_TOPIC, ANOMALY_TOPIC);
        processor.buildPipeline(builder);

        Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        // Create test topics
        JsonSerializer<StockPrice> stockPriceSerializer = new JsonSerializer<>(objectMapper);
        inputTopic = testDriver.createInputTopic(
                INPUT_TOPIC,
                new StringSerializer(),
                stockPriceSerializer
        );

        tickerOutputTopic = testDriver.createOutputTopic(
                TICKER_TOPIC,
                new StringDeserializer(),
                Serdes.Double().deserializer()
        );

        JsonDeserializer<AnomalyAlert> anomalyDeserializer = new JsonDeserializer<>(AnomalyAlert.class, objectMapper);
        anomalyDeserializer.addTrustedPackages("*");
        anomalyOutputTopic = testDriver.createOutputTopic(
                ANOMALY_TOPIC,
                new StringDeserializer(),
                anomalyDeserializer
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Anomaly detection: should detect price spike above 5% threshold")
    void shouldDetectAnomalyWhenPriceExceedsThreshold() {
        // Given: Initial price establishes baseline
        StockPrice initialPrice = new StockPrice("AAPL", 100.0, Instant.now());
        inputTopic.pipeInput("AAPL", initialPrice);

        // Clear any initial output
        while (!anomalyOutputTopic.isEmpty()) {
            anomalyOutputTopic.readKeyValue();
        }

        // When: Price jumps by more than 5%
        StockPrice spikePrice = new StockPrice("AAPL", 106.0, Instant.now());
        inputTopic.pipeInput("AAPL", spikePrice);

        // Then: Anomaly should be detected
        assertThat(anomalyOutputTopic.isEmpty()).isFalse();
        KeyValue<String, AnomalyAlert> alert = anomalyOutputTopic.readKeyValue();

        assertThat(alert.key).isEqualTo("AAPL");
        assertThat(alert.value).isNotNull();
        assertThat(alert.value.getSymbol()).isEqualTo("AAPL");
        assertThat(alert.value.getCurrentPrice()).isEqualTo(106.0);
        assertThat(alert.value.getMeanPrice()).isEqualTo(100.0);  // previousMax
        assertThat(alert.value.getStandardDeviation()).isEqualTo(6.0);  // deviation
        assertThat(alert.value.getAlertMessage()).contains("Price spike detected");
    }

    @Test
    @DisplayName("Anomaly detection: should NOT detect anomaly for price within threshold")
    void shouldNotDetectAnomalyWhenPriceWithinThreshold() {
        // Given: Initial price
        StockPrice initialPrice = new StockPrice("GOOG", 150.0, Instant.now());
        inputTopic.pipeInput("GOOG", initialPrice);

        // Clear initial output
        anomalyOutputTopic.readValuesToList();

        // When: Price increases by less than 5%
        StockPrice normalPrice = new StockPrice("GOOG", 155.0, Instant.now());
        inputTopic.pipeInput("GOOG", normalPrice);

        // Then: No anomaly should be detected (only null values forwarded)
        List<AnomalyAlert> alerts = anomalyOutputTopic.readValuesToList();
        assertThat(alerts).allMatch(alert -> alert == null);
    }

    @Test
    @DisplayName("Anomaly detection: should track max price per symbol independently")
    void shouldTrackMaxPricePerSymbolIndependently() {
        // Given: Different prices for different symbols
        inputTopic.pipeInput("AAPL", new StockPrice("AAPL", 100.0, Instant.now()));
        inputTopic.pipeInput("GOOG", new StockPrice("GOOG", 200.0, Instant.now()));

        anomalyOutputTopic.readValuesToList();

        // When: AAPL spikes but GOOG doesn't
        inputTopic.pipeInput("AAPL", new StockPrice("AAPL", 106.0, Instant.now()));
        inputTopic.pipeInput("GOOG", new StockPrice("GOOG", 205.0, Instant.now()));

        // Then: Only AAPL should trigger an anomaly
        List<KeyValue<String, AnomalyAlert>> results = anomalyOutputTopic.readKeyValuesToList();

        List<AnomalyAlert> nonNullAlerts = results.stream()
                .map(kv -> kv.value)
                .filter(alert -> alert != null)
                .toList();

        assertThat(nonNullAlerts).hasSize(1);
        assertThat(nonNullAlerts.get(0).getSymbol()).isEqualTo("AAPL");
    }

    @Test
    @DisplayName("Anomaly detection: should update max price in state store")
    void shouldUpdateMaxPriceInStateStore() {
        // Given: Initial price
        inputTopic.pipeInput("TSLA", new StockPrice("TSLA", 250.0, Instant.now()));
        anomalyOutputTopic.readValuesToList();

        // When: Higher price arrives
        inputTopic.pipeInput("TSLA", new StockPrice("TSLA", 270.0, Instant.now()));

        // Then: State store should be updated
        KeyValueStore<String, Double> stateStore =
                testDriver.getKeyValueStore("anomaly-state-store");

        Double maxPrice = stateStore.get("TSLA");
        assertThat(maxPrice).isEqualTo(270.0);
    }

    @Test
    @DisplayName("Anomaly detection: should handle first price for a symbol gracefully")
    void shouldHandleFirstPriceForSymbol() {
        // When: First price for a new symbol
        inputTopic.pipeInput("NVDA", new StockPrice("NVDA", 500.0, Instant.now()));

        // Then: Should not trigger anomaly (no previous max to compare)
        List<AnomalyAlert> alerts = anomalyOutputTopic.readValuesToList();
        assertThat(alerts).allMatch(alert -> alert == null);

        // And: Should store the price in state store
        KeyValueStore<String, Double> stateStore =
                testDriver.getKeyValueStore("anomaly-state-store");
        assertThat(stateStore.get("NVDA")).isEqualTo(500.0);
    }

    @Test
    @DisplayName("Ticker topology: should calculate average prices in windows")
    void shouldCalculateAveragePricesInWindows() {
        // Given: Multiple prices for the same symbol within a window
        Instant baseTime = Instant.now();

        inputTopic.pipeInput("MSFT", new StockPrice("MSFT", 300.0, baseTime), baseTime);
        inputTopic.pipeInput("MSFT", new StockPrice("MSFT", 310.0, baseTime.plusSeconds(1)), baseTime.plusSeconds(1));
        inputTopic.pipeInput("MSFT", new StockPrice("MSFT", 290.0, baseTime.plusSeconds(2)), baseTime.plusSeconds(2));

        // Advance time to trigger window processing
        inputTopic.pipeInput("DUMMY", new StockPrice("DUMMY", 0.0, baseTime.plusSeconds(15)), baseTime.plusSeconds(15));

        // Then: Should produce average values
        List<KeyValue<String, Double>> averages = tickerOutputTopic.readKeyValuesToList();

        // Filter for MSFT results
        List<Double> msftAverages = averages.stream()
                .filter(kv -> "MSFT".equals(kv.key))
                .map(kv -> kv.value)
                .toList();

        assertThat(msftAverages).isNotEmpty();
        // Average of 300, 310, 290 = 300
        assertThat(msftAverages).contains(300.0);
    }

    /**
     * Test implementation of StockProcessor that can be instantiated without Spring dependencies
     */
    private static class TestStockProcessor extends StockProcessor {
        private final String inputTopic;
        private final String tickerTopic;
        private final String anomalyTopic;

        public TestStockProcessor(String inputTopic, String tickerTopic, String anomalyTopic) {
            this.inputTopic = inputTopic;
            this.tickerTopic = tickerTopic;
            this.anomalyTopic = anomalyTopic;
        }

        // Override to bypass @Value injection and use constructor params
        public void buildPipeline(StreamsBuilder builder) {
            // Manually set the topic values that would normally be injected
            try {
                var inputTopicField = StockProcessor.class.getDeclaredField("inputTopic");
                inputTopicField.setAccessible(true);
                inputTopicField.set(this, inputTopic);

                var tickerTopicField = StockProcessor.class.getDeclaredField("tickerTopic");
                tickerTopicField.setAccessible(true);
                tickerTopicField.set(this, tickerTopic);

                var anomalyTopicField = StockProcessor.class.getDeclaredField("anomalyTopic");
                anomalyTopicField.setAccessible(true);
                anomalyTopicField.set(this, anomalyTopic);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set topic fields", e);
            }

            // Call the actual buildPipeline method
            super.buildPipeline(builder);
        }
    }
}
