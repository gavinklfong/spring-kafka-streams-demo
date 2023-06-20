package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import space.gavinklfong.demo.kafka.model.MedianStockPrice;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.math.BigDecimal;
import java.math.RoundingMode;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class StockPriceGroupByKeyTopology {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        KGroupedStream<String, StockPrice> groupedStream = builder.stream("stock-price",
                        Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()))
                .groupByKey(Grouped.with("stock-ticker-group-by-ticker", Serdes.String(), StockPriceSerdes.stockPrice()));

        KTable<String, Long> countByTicker = groupedStream.count(Named.as("count-by-ticker"));
        countByTicker.toStream()
                .peek((key, value) -> log.info("key: {}, value: {}", key, value))
                .to("count-by-ticker");

        return builder.build();
    }
}
