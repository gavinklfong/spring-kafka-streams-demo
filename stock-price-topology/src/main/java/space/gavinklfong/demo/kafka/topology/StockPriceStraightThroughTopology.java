package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class StockPriceStraightThroughTopology {

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("stock-price", Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()))
                .peek((key, value) -> log.info("key: {}, value: {}", key, value))
                .to("straight-through-stock-price", Produced.with(Serdes.String(), StockPriceSerdes.stockPrice()));

        return builder.build();
    }

}
