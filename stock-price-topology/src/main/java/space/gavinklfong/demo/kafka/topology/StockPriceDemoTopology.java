package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.model.MedianStockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.math.BigDecimal;
import java.math.RoundingMode;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class StockPriceDemoTopology {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("stock-price",
                        Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()))
                .peek((key, value) -> log.info("input - key: {}, value: {}", key, value), Named.as("log-input"))
                .filterNot((key, value) -> key.equals("IBM"), Named.as("filter-not-IBM"))
                .mapValues(StockPriceDemoTopology::mapToMedianStockPrice, Named.as("map-to-median-stock-price"))
                .peek((key, value) -> log.info("output - key: {}, value: {}", key, value), Named.as("log-output"))
                .to("transformed-stock-price",
                        Produced.with(Serdes.String(), StockPriceSerdes.medianStockPrice()));

        return builder.build();
    }

    private static MedianStockPrice mapToMedianStockPrice(StockPrice stockPrice) {
        return MedianStockPrice.builder()
                .timestamp(stockPrice.getTimestamp())
                .median((BigDecimal.valueOf(stockPrice.getHigh()).min(BigDecimal.valueOf(stockPrice.getLow())))
                        .divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP))
                .volume(stockPrice.getVolume())
                .build();
    }
}
