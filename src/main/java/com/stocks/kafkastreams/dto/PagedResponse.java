package com.stocks.kafkastreams.dto;

import lombok.Data;
import java.util.List;

/**
 * Generic paginated response wrapper.
 * Provides pagination metadata along with the actual data.
 *
 * @param <T> The type of content being paginated
 */
@Data
public class PagedResponse<T> {
    private List<T> content;           // The actual data for this page
    private int page;                  // Current page number (0-indexed)
    private int size;                  // Items per page (requested)
    private long totalElements;        // Total number of items across all pages
    private int totalPages;            // Total number of pages
    private boolean isFirst;           // Is this the first page?
    private boolean isLast;            // Is this the last page?
    private boolean isEmpty;           // Is the content empty?


    public PagedResponse(List<T> content, int page, int size, long totalElements) {
        this.content = content;
        this.page = page;
        this.size = size;
        this.totalElements = totalElements;

        this.totalPages = (int) Math.ceil((double) totalElements / size);

        this.isFirst = (page == 0);
        this.isLast = (page >= totalPages - 1);
        this.isEmpty = content.isEmpty();
    }

    public static <T> PagedResponse<T> empty(int page, int size) {
        return new PagedResponse<>(List.of(), page, size, 0);
    }
}