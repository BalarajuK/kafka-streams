package com.meghasys.kstream.kafka_streams.controllers;

import com.meghasys.kstream.kafka_streams.dto.StockCount;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
public class StockController {

    @Autowired
    private StreamsBuilderFactoryBean kStreamBean;

    @GetMapping("/count")
    public ResponseEntity<StockCount> getStockCount(@RequestParam("stock-name") String name){
        KafkaStreams kStream = kStreamBean.getKafkaStreams();

        ReadOnlyKeyValueStore<String, Long> counts = kStream.store(StoreQueryParameters.fromNameAndType("stock-count", QueryableStoreTypes.keyValueStore()));
        long stockCount1 = counts.get(name);
        StockCount stockCount = new StockCount();
        stockCount.setStockCount(stockCount1);
        return ResponseEntity.ok(stockCount);

    }
}
