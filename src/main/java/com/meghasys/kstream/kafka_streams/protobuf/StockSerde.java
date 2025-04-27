package com.meghasys.kstream.kafka_streams.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class StockSerde implements Serde<StockPriceOuterClass.StockPrice> {

    @Override
    public Serializer<StockPriceOuterClass.StockPrice> serializer() {
        return (topic, data) -> data.toByteArray();
    }

    @Override
    public Deserializer<StockPriceOuterClass.StockPrice> deserializer() {
        return (topic, bytes) -> {
            try {
                return StockPriceOuterClass.StockPrice.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        };
    }


}
