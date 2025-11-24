package com.stocks.kafkastreams.controller;

import com.stocks.kafkastreams.dto.PagedResponse;
import com.stocks.kafkastreams.dto.StockMaxPriceResponse;
import com.stocks.kafkastreams.service.StockQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for StockQueryController REST endpoints.
 * Uses MockMvc to test HTTP layer without starting full server.
 */
@WebMvcTest(StockQueryController.class)
class StockQueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StockQueryService stockQueryService;

    @Test
    @DisplayName("GET /api/stocks/health - should return UP when streams are ready")
    void healthEndpoint_shouldReturnUpWhenStreamsReady() throws Exception {
        // Given
        when(stockQueryService.isStreamsReady()).thenReturn(true);

        // When & Then
        mockMvc.perform(get("/api/stocks/health"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.service").value("Stock Query API"))
                .andExpect(jsonPath("$.streamsReady").value(true));
    }

    @Test
    @DisplayName("GET /api/stocks/health - should return DOWN when streams are not ready")
    void healthEndpoint_shouldReturnDownWhenStreamsNotReady() throws Exception {
        // Given
        when(stockQueryService.isStreamsReady()).thenReturn(false);

        // When & Then
        mockMvc.perform(get("/api/stocks/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DOWN"))
                .andExpect(jsonPath("$.streamsReady").value(false));
    }

    @Test
    @DisplayName("GET /api/stocks/{symbol}/max-price - should return max price for valid symbol")
    void getMaxPrice_shouldReturnPriceForValidSymbol() throws Exception {
        // Given
        String symbol = "AAPL";
        StockMaxPriceResponse response = new StockMaxPriceResponse(
                symbol,
                150.50,
                "Current historical maximum"
        );
        when(stockQueryService.getMaxPriceBySymbol(symbol)).thenReturn(Optional.of(response));

        // When & Then
        mockMvc.perform(get("/api/stocks/{symbol}/max-price", symbol))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.symbol").value("AAPL"))
                .andExpect(jsonPath("$.maxPrice").value(150.50))
                .andExpect(jsonPath("$.message").value("Current historical maximum"));
    }

    @Test
    @DisplayName("GET /api/stocks/{symbol}/max-price - should return 404 for unknown symbol")
    void getMaxPrice_shouldReturn404ForUnknownSymbol() throws Exception {
        // Given
        String symbol = "UNKNOWN";
        when(stockQueryService.getMaxPriceBySymbol(symbol)).thenReturn(Optional.empty());

        // When & Then
        mockMvc.perform(get("/api/stocks/{symbol}/max-price", symbol))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.symbol").value("UNKNOWN"))
                .andExpect(jsonPath("$.maxPrice").isEmpty())
                .andExpect(jsonPath("$.message").value("Symbol not found or no data available"));
    }

    @Test
    @DisplayName("GET /api/stocks/max-prices - should return paginated results with default params")
    void getAllMaxPrices_shouldReturnPaginatedResultsWithDefaults() throws Exception {
        // Given
        List<StockMaxPriceResponse> content = List.of(
                new StockMaxPriceResponse("AAPL", 150.0, "Historical maximum"),
                new StockMaxPriceResponse("GOOG", 2800.0, "Historical maximum")
        );
        PagedResponse<StockMaxPriceResponse> pagedResponse = new PagedResponse<>(content, 0, 20, 2);

        when(stockQueryService.getAllMaxPrices(anyInt(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(pagedResponse);

        // When & Then
        mockMvc.perform(get("/api/stocks/max-prices"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content", hasSize(2)))
                .andExpect(jsonPath("$.content[0].symbol").value("AAPL"))
                .andExpect(jsonPath("$.content[1].symbol").value("GOOG"))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(20))
                .andExpect(jsonPath("$.totalElements").value(2));
    }

    @Test
    @DisplayName("GET /api/stocks/max-prices - should accept pagination parameters")
    void getAllMaxPrices_shouldAcceptPaginationParams() throws Exception {
        // Given
        List<StockMaxPriceResponse> content = List.of(
                new StockMaxPriceResponse("TSLA", 250.0, "Historical maximum")
        );
        PagedResponse<StockMaxPriceResponse> pagedResponse = new PagedResponse<>(content, 1, 10, 15);

        when(stockQueryService.getAllMaxPrices(1, 10, "symbol", true))
                .thenReturn(pagedResponse);

        // When & Then
        mockMvc.perform(get("/api/stocks/max-prices")
                        .param("page", "1")
                        .param("size", "10")
                        .param("sort", "symbol,asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(10));
    }

    @Test
    @DisplayName("GET /api/stocks/max-prices - should accept sort by price descending")
    void getAllMaxPrices_shouldSortByPriceDescending() throws Exception {
        // Given
        List<StockMaxPriceResponse> content = List.of(
                new StockMaxPriceResponse("GOOG", 2800.0, "Historical maximum"),
                new StockMaxPriceResponse("AAPL", 150.0, "Historical maximum")
        );
        PagedResponse<StockMaxPriceResponse> pagedResponse = new PagedResponse<>(content, 0, 20, 2);

        when(stockQueryService.getAllMaxPrices(0, 20, "price", false))
                .thenReturn(pagedResponse);

        // When & Then
        mockMvc.perform(get("/api/stocks/max-prices")
                        .param("sort", "price,desc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].maxPrice").value(2800.0))
                .andExpect(jsonPath("$.content[1].maxPrice").value(150.0));
    }

    @Test
    @DisplayName("GET /api/stocks/max-prices - should return empty page when no data")
    void getAllMaxPrices_shouldReturnEmptyPageWhenNoData() throws Exception {
        // Given
        PagedResponse<StockMaxPriceResponse> emptyResponse = new PagedResponse<>(List.of(), 0, 20, 0);
        when(stockQueryService.getAllMaxPrices(anyInt(), anyInt(), anyString(), anyBoolean()))
                .thenReturn(emptyResponse);

        // When & Then
        mockMvc.perform(get("/api/stocks/max-prices"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(0)))
                .andExpect(jsonPath("$.totalElements").value(0));
    }

    @Test
    @DisplayName("GET /api/stocks/stats - should return store statistics")
    void getStats_shouldReturnStoreStatistics() throws Exception {
        // Given
        StockQueryService.StoreStats stats = new StockQueryService.StoreStats(
                42L,
                "RUNNING",
                "anomaly-state-store"
        );
        when(stockQueryService.getStoreStats()).thenReturn(stats);

        // When & Then
        mockMvc.perform(get("/api/stocks/stats"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.storeSize").value(42))
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(jsonPath("$.storeName").value("anomaly-state-store"));
    }
}
