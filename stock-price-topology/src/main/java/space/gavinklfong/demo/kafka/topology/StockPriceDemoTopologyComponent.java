package space.gavinklfong.demo.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import space.gavinklfong.demo.kafka.model.MedianStockPrice;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
@Slf4j
public class StockPriceDemoTopologyComponent {

    @Autowired
    public void build(StreamsBuilder builder) {
        builder.stream("stock-price",
                        Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()))
                .peek((key, value) -> log.info("input - key: {}, value: {}", key, value), Named.as("log-input"))
                .filterNot((key, value) -> key.equals("IBM"), Named.as("filter-not-IBM"))
                .mapValues(this::mapToMedianStockPrice, Named.as("map-to-median-stock-price"))
                .peek((key, value) -> log.info("output - key: {}, value: {}", key, value), Named.as("log-output"))
                .to("transformed-stock-price",
                        Produced.with(Serdes.String(), StockPriceSerdes.medianStockPrice()));
    }

    private MedianStockPrice mapToMedianStockPrice(StockPrice stockPrice) {
        return MedianStockPrice.builder()
                .timestamp(stockPrice.getTimestamp())
                .median((BigDecimal.valueOf(stockPrice.getHigh()).min(BigDecimal.valueOf(stockPrice.getLow())))
                        .divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP))
                .volume(stockPrice.getVolume())
                .build();
    }
}
