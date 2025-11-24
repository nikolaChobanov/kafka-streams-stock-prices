package com.stocks.kafkastreams.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;


@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * Triggered by: Service-level validation failures
     * HTTP Status: 400 Bad Request
     *
     * Example scenarios:
     * - Negative page number
     * - Page size exceeds maximum
     * - Invalid sort field
     * - Null or empty required parameters
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleValidationError(IllegalArgumentException ex) {
        log.warn("Validation error: {}", ex.getMessage());

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("timestamp", Instant.now());
        errorResponse.put("status", HttpStatus.BAD_REQUEST.value());
        errorResponse.put("error", "Bad Request");
        errorResponse.put("message", ex.getMessage());

        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(errorResponse);
    }

    /**
     * Triggered by: Kafka Streams in wrong state (not RUNNING/REBALANCING)
     * HTTP Status: 503 Service Unavailable
     *
     * Example scenarios:
     * - Application just started (streams initializing)
     * - Rebalancing in progress
     * - Kafka connection issues
     * - State store not accessible
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleServiceUnavailable(IllegalStateException ex) {
        log.error("Service unavailable: {}", ex.getMessage());

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("timestamp", Instant.now());
        errorResponse.put("status", HttpStatus.SERVICE_UNAVAILABLE.value());
        errorResponse.put("error", "Service Unavailable");
        errorResponse.put("message", "The service is temporarily unavailable. Please try again later.");
        errorResponse.put("details", ex.getMessage());

        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(errorResponse);
    }

    /**
     * Triggered by: Spring parameter conversion failures
     * HTTP Status: 400 Bad Request
     *
     * Example scenarios:
     * - /api/stocks/max-prices?page=abc (not a number)
     * - /api/stocks/max-prices?size=xyz (not a number)
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        log.warn("Type mismatch error for parameter '{}': {}", ex.getName(), ex.getMessage());

        String message = String.format(
            "Invalid value '%s' for parameter '%s'. Expected type: %s",
            ex.getValue(),
            ex.getName(),
            ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown"
        );

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("timestamp", Instant.now());
        errorResponse.put("status", HttpStatus.BAD_REQUEST.value());
        errorResponse.put("error", "Bad Request");
        errorResponse.put("message", message);

        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(errorResponse);
    }

    /**
     * This is a catch-all handler for any exceptions not explicitly handled above.
     * HTTP Status: 500 Internal Server Error
     *
     * Example scenarios:
     * - NullPointerException
     * - Unexpected runtime errors
     * - Unhandled edge cases
     *
     * Important: Don't expose internal error details to clients in production!
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericError(Exception ex) {
        log.error("Unexpected error occurred", ex);

        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("timestamp", Instant.now());
        errorResponse.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
        errorResponse.put("error", "Internal Server Error");
        errorResponse.put("message", "An unexpected error occurred. Please contact support if the problem persists.");

        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(errorResponse);
    }
}
