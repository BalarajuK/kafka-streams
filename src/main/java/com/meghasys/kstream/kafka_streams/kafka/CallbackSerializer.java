package com.meghasys.kstream.kafka_streams.kafka;

import com.meghasys.kstream.kafka_streams.protobuf.StockPriceOuterClass;
import org.apache.kafka.common.serialization.Serializer;

public class CallbackSerializer implements Serializer<StockPriceOuterClass.StockPrice> {

    @Override
    public byte[] serialize(String s, StockPriceOuterClass.StockPrice stockPrice) {
        return stockPrice.toByteArray();
    }
}
