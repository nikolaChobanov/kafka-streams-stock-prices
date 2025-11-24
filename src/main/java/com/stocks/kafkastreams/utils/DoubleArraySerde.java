package com.stocks.kafkastreams.utils;

import org.apache.kafka.common.serialization.Serdes;

public class DoubleArraySerde extends Serdes.WrapperSerde<double[]> {
    public DoubleArraySerde() {
        super(
                (topic, data) -> { // Serializer
                    if (data == null) return null;
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(16);
                    buf.putDouble(data[0]).putDouble(data[1]);
                    return buf.array();
                },
                (topic, data) -> { // Deserializer
                    if (data == null) return null;
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
                    return new double[]{buf.getDouble(), buf.getDouble()};
                }
        );
    }
}

