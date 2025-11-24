package com.stocks.kafkastreams.controller;

import com.stocks.kafkastreams.dto.PagedResponse;
import com.stocks.kafkastreams.dto.StockMaxPriceResponse;
import com.stocks.kafkastreams.service.StockQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

/**
 * REST controller for querying stock data from Kafka Streams state stores.
 */
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
@Slf4j
public class StockQueryController {

    private final StockQueryService stockQueryService;

    /**
     * Health check endpoint.
     * <p>
     * Example: GET /api/stocks/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean ready = stockQueryService.isStreamsReady();
        return ResponseEntity.ok(Map.of(
                "status", ready ? "UP" : "DOWN",
                "service", "Stock Query API",
                "streamsReady", ready
        ));
    }

    /**
     * Get maximum historical price for a specific stock symbol.
     * <p>
     * Example: GET /api/stocks/GOOG/max-price
     */
    @GetMapping("/{symbol}/max-price")
    public ResponseEntity<StockMaxPriceResponse> getMaxPrice(@PathVariable String symbol) {
        Optional<StockMaxPriceResponse> result = stockQueryService.getMaxPriceBySymbol(symbol);

        return result
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(new StockMaxPriceResponse(
                                symbol,
                                null,
                                "Symbol not found or no data available"
                        )));
    }

    /**
     * Get all maximum prices with pagination and sorting.
     * <p>
     * Example: GET /api/stocks/max-prices?page=0&size=20&sort=price,desc
     * <p>
     * Query Parameters:
     * - page: Page number (0-indexed, default: 0)
     * - size: Items per page (default: 20, max: 100)
     * - sort: Sort field and direction (format: "field,direction")
     * Supported: "symbol,asc", "symbol,desc", "price,asc", "price,desc"
     */
    @GetMapping("/max-prices")
    public ResponseEntity<PagedResponse<StockMaxPriceResponse>> getAllMaxPrices(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String sort
    ) {
        String sortField = "symbol";
        boolean ascending = true;

        if (sort != null && !sort.isBlank()) {
            String[] parts = sort.split(",");
            sortField = parts[0].trim().toLowerCase();
            if (parts.length > 1) {
                ascending = parts[1].trim().equalsIgnoreCase("asc");
            }
        }

        PagedResponse<StockMaxPriceResponse> response =
                stockQueryService.getAllMaxPrices(page, size, sortField, ascending);

        return ResponseEntity.ok(response);
    }

    /**
     * Get statistics about the state store.
     * <p>
     * Example: GET /api/stocks/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<StockQueryService.StoreStats> getStats() {
        StockQueryService.StoreStats stats = stockQueryService.getStoreStats();
        return ResponseEntity.ok(stats);
    }
}
