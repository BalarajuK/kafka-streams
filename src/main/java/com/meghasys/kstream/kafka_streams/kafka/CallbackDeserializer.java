package com.meghasys.kstream.kafka_streams.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.meghasys.kstream.kafka_streams.protobuf.StockPriceOuterClass;
import org.apache.kafka.common.serialization.Deserializer;


public class CallbackDeserializer implements Deserializer<StockPriceOuterClass.StockPrice> {

    @Override
    public StockPriceOuterClass.StockPrice deserialize(String s, byte[] bytes) {
        try {
            return StockPriceOuterClass.StockPrice.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            System.out.println("Problem in parsing");
            return null;
        }
    }
}