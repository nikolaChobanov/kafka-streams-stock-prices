package com.stocks.kafkastreams.service;

import com.stocks.kafkastreams.dto.PagedResponse;
import com.stocks.kafkastreams.dto.StockMaxPriceResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This service handles all business logic for querying stock data from state stores.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StockQueryServiceImpl implements StockQueryService {

    private static final String STATE_STORE_NAME = "anomaly-state-store";
    private static final int MAX_PAGE_SIZE = 100;

    private final StreamsBuilderFactoryBean factoryBean;

    @Override
    public Optional<StockMaxPriceResponse> getMaxPriceBySymbol(String symbol) {
        log.debug("Querying max price for symbol: {}", symbol);

        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol cannot be null or empty");
        }

        ReadOnlyKeyValueStore<String, Double> store = getStateStore();
        Double maxPrice = store.get(symbol.trim().toUpperCase());

        if (maxPrice != null) {
            log.info("Found max price for {}: ${}", symbol, maxPrice);
            return Optional.of(new StockMaxPriceResponse(
                    symbol.toUpperCase(),
                    maxPrice,
                    "Current historical maximum"
            ));
        }

        log.debug("No data found for symbol: {}", symbol);
        return Optional.empty();
    }

    @Override
    public PagedResponse<StockMaxPriceResponse> getAllMaxPrices(
            int page,
            int size,
            String sortField,
            boolean ascending
    ) {
        log.debug("Querying all max prices: page={}, size={}, sort={}:{}",
                page, size, sortField, ascending ? "ASC" : "DESC");

        validatePaginationParams(page, size);
        validateSortField(sortField);

        ReadOnlyKeyValueStore<String, Double> store = getStateStore();
        List<StockMaxPriceResponse> allItems = collectAllItems(store);
        sortItems(allItems, sortField, ascending);
        List<StockMaxPriceResponse> pageContent = extractPage(allItems, page, size);

        PagedResponse<StockMaxPriceResponse> response = new PagedResponse<>(
                pageContent,
                page,
                size,
                allItems.size()
        );

        log.info("Returning page {} of {} total stocks", page, allItems.size());
        return response;
    }

    @Override
    public boolean isStreamsReady() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return false;
        }
        KafkaStreams.State state = streams.state();
        return state.isRunningOrRebalancing();
    }

    @Override
    public StoreStats getStoreStats() {
        KafkaStreams streams = getKafkaStreams();
        ReadOnlyKeyValueStore<String, Double> store = getStateStore();

        long storeSize = store.approximateNumEntries();
        String state = streams.state().toString();

        return new StoreStats(storeSize, state, STATE_STORE_NAME);
    }

    private KafkaStreams getKafkaStreams() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            throw new IllegalStateException("Kafka Streams is not available");
        }
        return streams;
    }

    private ReadOnlyKeyValueStore<String, Double> getStateStore() {
        KafkaStreams streams = getKafkaStreams();

        try {
            return streams.store(
                    StoreQueryParameters.fromNameAndType(
                            STATE_STORE_NAME,
                            QueryableStoreTypes.keyValueStore()
                    )
            );
        } catch (Exception e) {
            log.error("Failed to access state store: {}", e.getMessage());
            throw new IllegalStateException("State store is not ready: " + e.getMessage(), e);
        }
    }

    private void validatePaginationParams(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("Page number must be >= 0");
        }
        if (size <= 0 || size > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Size must be between 1 and %d", MAX_PAGE_SIZE)
            );
        }
    }

    private void validateSortField(String sortField) {
        if (sortField != null &&
                !sortField.equals("symbol") &&
                !sortField.equals("price")) {
            throw new IllegalArgumentException(
                    "Sort field must be 'symbol' or 'price', got: " + sortField
            );
        }
    }

    private List<StockMaxPriceResponse> collectAllItems(
            ReadOnlyKeyValueStore<String, Double> store
    ) {
        List<StockMaxPriceResponse> items = new ArrayList<>();

        try (KeyValueIterator<String, Double> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Double> entry = iterator.next();
                items.add(new StockMaxPriceResponse(
                        entry.key,
                        entry.value,
                        "Historical maximum"
                ));
            }
        }

        return items;
    }

    private void sortItems(
            List<StockMaxPriceResponse> items,
            String sortField,
            boolean ascending
    ) {
        items.sort((a, b) -> {
            int comparison;
            if ("price".equals(sortField)) {
                comparison = Double.compare(a.getMaxPrice(), b.getMaxPrice());
            } else {
                comparison = a.getSymbol().compareTo(b.getSymbol());
            }
            return ascending ? comparison : -comparison;
        });
    }

    private List<StockMaxPriceResponse> extractPage(
            List<StockMaxPriceResponse> allItems,
            int page,
            int size
    ) {
        int fromIndex = page * size;

        if (fromIndex >= allItems.size()) {
            return List.of();
        }

        int toIndex = Math.min(fromIndex + size, allItems.size());
        return allItems.subList(fromIndex, toIndex);
    }
}
