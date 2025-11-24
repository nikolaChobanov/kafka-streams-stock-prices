package com.stocks.kafkastreams.service;

import com.stocks.kafkastreams.dto.PagedResponse;
import com.stocks.kafkastreams.dto.StockMaxPriceResponse;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StockQueryServiceImpl.
 * Tests business logic for querying state stores.
 */
@ExtendWith(MockitoExtension.class)
class StockQueryServiceImplTest {

    @Mock
    private StreamsBuilderFactoryBean factoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, Double> stateStore;

    @Mock
    private KeyValueIterator<String, Double> iterator;

    private StockQueryServiceImpl service;

    @BeforeEach
    void setUp() {
        // Reset all mocks to avoid state leaking between tests
        reset(factoryBean, kafkaStreams, stateStore, iterator);

        service = new StockQueryServiceImpl(factoryBean);
        lenient().when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        lenient().when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(stateStore);
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should return price when symbol exists")
    void getMaxPriceBySymbol_shouldReturnPriceWhenSymbolExists() {
        // Given
        String symbol = "AAPL";
        Double price = 150.50;
        when(stateStore.get("AAPL")).thenReturn(price);

        // When
        Optional<StockMaxPriceResponse> result = service.getMaxPriceBySymbol(symbol);

        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getSymbol()).isEqualTo("AAPL");
        assertThat(result.get().getMaxPrice()).isEqualTo(150.50);
        assertThat(result.get().getMessage()).isEqualTo("Current historical maximum");
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should return empty when symbol not found")
    void getMaxPriceBySymbol_shouldReturnEmptyWhenSymbolNotFound() {
        // Given
        String symbol = "UNKNOWN";
        when(stateStore.get("UNKNOWN")).thenReturn(null);

        // When
        Optional<StockMaxPriceResponse> result = service.getMaxPriceBySymbol(symbol);

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should convert symbol to uppercase")
    void getMaxPriceBySymbol_shouldConvertSymbolToUppercase() {
        // Given
        String symbol = "aapl";
        when(stateStore.get("AAPL")).thenReturn(150.0);

        // When
        Optional<StockMaxPriceResponse> result = service.getMaxPriceBySymbol(symbol);

        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getSymbol()).isEqualTo("AAPL");
        verify(stateStore).get("AAPL");
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should trim whitespace from symbol")
    void getMaxPriceBySymbol_shouldTrimWhitespace() {
        // Given
        String symbol = "  AAPL  ";
        when(stateStore.get("AAPL")).thenReturn(150.0);

        // When
        service.getMaxPriceBySymbol(symbol);

        // Then
        verify(stateStore).get("AAPL");
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should throw exception for null symbol")
    void getMaxPriceBySymbol_shouldThrowExceptionForNullSymbol() {
        // When & Then
        assertThatThrownBy(() -> service.getMaxPriceBySymbol(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbol cannot be null or empty");
    }

    @Test
    @DisplayName("getMaxPriceBySymbol - should throw exception for blank symbol")
    void getMaxPriceBySymbol_shouldThrowExceptionForBlankSymbol() {
        // When & Then
        assertThatThrownBy(() -> service.getMaxPriceBySymbol("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Symbol cannot be null or empty");
    }

    @Test
    @DisplayName("getAllMaxPrices - should return paginated results sorted by symbol")
    void getAllMaxPrices_shouldReturnPaginatedResultsSortedBySymbol() {
        // Given
        List<KeyValue<String, Double>> storeData = List.of(
                new KeyValue<>("GOOG", 2800.0),
                new KeyValue<>("AAPL", 150.0),
                new KeyValue<>("TSLA", 250.0)
        );
        mockStoreIterator(storeData);

        // When
        PagedResponse<StockMaxPriceResponse> result =
                service.getAllMaxPrices(0, 2, "symbol", true);

        // Then
        assertThat(result.getContent()).hasSize(2);
        assertThat(result.getContent().get(0).getSymbol()).isEqualTo("AAPL");
        assertThat(result.getContent().get(1).getSymbol()).isEqualTo("GOOG");
        assertThat(result.getPage()).isEqualTo(0);
        assertThat(result.getSize()).isEqualTo(2);
        assertThat(result.getTotalElements()).isEqualTo(3);
        assertThat(result.getTotalPages()).isEqualTo(2);
    }

    @Test
    @DisplayName("getAllMaxPrices - should sort by price descending")
    void getAllMaxPrices_shouldSortByPriceDescending() {
        // Given
        List<KeyValue<String, Double>> storeData = List.of(
                new KeyValue<>("AAPL", 150.0),
                new KeyValue<>("GOOG", 2800.0),
                new KeyValue<>("TSLA", 250.0)
        );
        mockStoreIterator(storeData);

        // When
        PagedResponse<StockMaxPriceResponse> result =
                service.getAllMaxPrices(0, 10, "price", false);

        // Then
        assertThat(result.getContent()).hasSize(3);
        assertThat(result.getContent().get(0).getSymbol()).isEqualTo("GOOG");
        assertThat(result.getContent().get(0).getMaxPrice()).isEqualTo(2800.0);
        assertThat(result.getContent().get(1).getSymbol()).isEqualTo("TSLA");
        assertThat(result.getContent().get(2).getSymbol()).isEqualTo("AAPL");
    }

    @Test
    @DisplayName("getAllMaxPrices - should return empty page when page number exceeds data")
    void getAllMaxPrices_shouldReturnEmptyPageWhenPageExceedsData() {
        // Given
        List<KeyValue<String, Double>> storeData = List.of(
                new KeyValue<>("AAPL", 150.0)
        );
        mockStoreIterator(storeData);

        // When
        PagedResponse<StockMaxPriceResponse> result =
                service.getAllMaxPrices(5, 10, "symbol", true);

        // Then
        assertThat(result.getContent()).isEmpty();
        assertThat(result.getTotalElements()).isEqualTo(1);
        assertThat(result.getPage()).isEqualTo(5);
    }

    @Test
    @DisplayName("getAllMaxPrices - should throw exception for negative page number")
    void getAllMaxPrices_shouldThrowExceptionForNegativePage() {
        // When & Then
        assertThatThrownBy(() -> service.getAllMaxPrices(-1, 10, "symbol", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Page number must be >= 0");
    }

    @Test
    @DisplayName("getAllMaxPrices - should throw exception for invalid size")
    void getAllMaxPrices_shouldThrowExceptionForInvalidSize() {
        // When & Then
        assertThatThrownBy(() -> service.getAllMaxPrices(0, 0, "symbol", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Size must be between 1 and 100");

        assertThatThrownBy(() -> service.getAllMaxPrices(0, 101, "symbol", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Size must be between 1 and 100");
    }

    @Test
    @DisplayName("getAllMaxPrices - should throw exception for invalid sort field")
    void getAllMaxPrices_shouldThrowExceptionForInvalidSortField() {
        // Given
        mockStoreIterator(List.of());

        // When & Then
        assertThatThrownBy(() -> service.getAllMaxPrices(0, 10, "invalid", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Sort field must be 'symbol' or 'price'");
    }

    @Test
    @DisplayName("isStreamsReady - should return true when streams are running")
    void isStreamsReady_shouldReturnTrueWhenStreamsRunning() {
        // Given
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        // When
        boolean result = service.isStreamsReady();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("isStreamsReady - should return true when streams are rebalancing")
    void isStreamsReady_shouldReturnTrueWhenStreamsRebalancing() {
        // Given
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        // When
        boolean result = service.isStreamsReady();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("isStreamsReady - should return false when streams are not running")
    void isStreamsReady_shouldReturnFalseWhenStreamsNotRunning() {
        // Given
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.PENDING_SHUTDOWN);

        // When
        boolean result = service.isStreamsReady();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("isStreamsReady - should return false when streams instance is null")
    void isStreamsReady_shouldReturnFalseWhenStreamsNull() {
        // Given
        when(factoryBean.getKafkaStreams()).thenReturn(null);

        // When
        boolean result = service.isStreamsReady();

        // Then
        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("getStoreStats - should return correct statistics")
    void getStoreStats_shouldReturnCorrectStatistics() {
        // Given
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(stateStore.approximateNumEntries()).thenReturn(42L);

        // When
        StockQueryService.StoreStats stats = service.getStoreStats();

        // Then
        assertThat(stats.storeSize()).isEqualTo(42L);
        assertThat(stats.state()).isEqualTo("RUNNING");
        assertThat(stats.storeName()).isEqualTo("anomaly-state-store");
    }

    @Test
    @DisplayName("getStoreStats - should throw exception when streams not available")
    void getStoreStats_shouldThrowExceptionWhenStreamsNotAvailable() {
        // Given
        when(factoryBean.getKafkaStreams()).thenReturn(null);

        // When & Then
        assertThatThrownBy(() -> service.getStoreStats())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka Streams is not available");
    }

    /**
     * Helper method to mock the KeyValueIterator for state store queries
     */
    private void mockStoreIterator(List<KeyValue<String, Double>> data) {
        lenient().when(stateStore.all()).thenReturn(iterator);

        // Mock hasNext() to return true for each element, then false
        if (data.isEmpty()) {
            lenient().when(iterator.hasNext()).thenReturn(false);
        } else {
            // Build the complete sequence: true for each element, then false
            Boolean[] hasNextResults = new Boolean[data.size()];
            for (int i = 0; i < data.size() - 1; i++) {
                hasNextResults[i] = true;
            }
            hasNextResults[data.size() - 1] = false;
            lenient().when(iterator.hasNext()).thenReturn(true, hasNextResults);
        }

        // Mock next() to return each KeyValue in sequence
        if (!data.isEmpty()) {
            KeyValue<String, Double> first = data.get(0);
            KeyValue<String, Double>[] rest = data.subList(1, data.size()).toArray(new KeyValue[0]);
            lenient().when(iterator.next()).thenReturn(first, rest);
        }
    }
}
