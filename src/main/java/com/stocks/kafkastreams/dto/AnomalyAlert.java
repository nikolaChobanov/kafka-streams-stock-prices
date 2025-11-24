package com.stocks.kafkastreams.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyAlert {

    private String ticker;
    private double currentPrice;
    private double meanPrice;
    private double standardDeviation;
    private String alertMessage;
    private Instant timestamp;

}