package com.stocks.kafkastreams.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockMaxPriceResponse {

    String symbol;
    Double maxPrice;
    String message;

}
