package com.meghasys.kstream.kafka_streams.consumers;

import com.meghasys.kstream.kafka_streams.constants.AppConstants;
import com.meghasys.kstream.kafka_streams.protobuf.StockPriceOuterClass;
import com.meghasys.kstream.kafka_streams.protobuf.StockSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StockCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, StockPriceOuterClass.StockPrice> messageStream = streamsBuilder
          .stream(AppConstants.STOCK_PRICE, Consumed.with(Serdes.String(), new StockSerde()));

        KTable<String, Long> wordCounts = messageStream
          .mapValues((key, stock)->{
              return stock.getName();
          })
          .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
          .count(Materialized.as("stock-count"));

        //wordCounts.toStream().to("stock-count");
    }
}