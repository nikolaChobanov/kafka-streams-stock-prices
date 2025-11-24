package com.stocks.kafkastreams.service;

import com.stocks.kafkastreams.dto.PagedResponse;
import com.stocks.kafkastreams.dto.StockMaxPriceResponse;

import java.util.Optional;

/**
 * Service for querying stock data from Kafka Streams state stores.
 *
 * This interface defines the contract for interactive queries
 * against the running Kafka Streams application.
 */
public interface StockQueryService {

    /**
     * Get the maximum historical price for a specific stock symbol.
     *
     * @param symbol The stock symbol (e.g., "GOOG")
     * @return Optional containing the max price response, or empty if not found
     * @throws IllegalStateException if Kafka Streams is not ready
     */
    Optional<StockMaxPriceResponse> getMaxPriceBySymbol(String symbol);

    /**
     * Get all maximum prices with pagination and optional sorting.
     *
     * @param page Page number (0-indexed)
     * @param size Items per page
     * @param sortField Field to sort by ("symbol" or "price")
     * @param ascending Sort direction (true for ascending, false for descending)
     * @return Paginated response containing stock max prices
     * @throws IllegalArgumentException if parameters are invalid
     * @throws IllegalStateException if Kafka Streams is not ready
     */
    PagedResponse<StockMaxPriceResponse> getAllMaxPrices(
        int page,
        int size,
        String sortField,
        boolean ascending
    );

    /**
     * Check if the Kafka Streams application is ready to serve queries.
     *
     * @return true if streams are in RUNNING or REBALANCING state
     */
    boolean isStreamsReady();

    /**
     * Get statistics about the state store.
     *
     * @return Statistics including store size, state, etc.
     * @throws IllegalStateException if Kafka Streams is not ready
     */
    StoreStats getStoreStats();

    /**
     * Simple DTO for store statistics.
     */
    record StoreStats(
        long storeSize,
        String state,
        String storeName
    ) {}
}
