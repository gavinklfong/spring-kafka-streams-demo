package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import space.gavinklfong.demo.kafka.model.CountAndSum;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.model.TickerAndTimestamp;
import space.gavinklfong.demo.kafka.topology.processor.StockPriceTimestampExtractor;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.apache.kafka.streams.kstream.Materialized.as;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StockPriceMovingAverageWindowTopology {

    private static final int MINUTE_INTERVAL = 10;
    private static final String INPUT_TOPIC = "stock-price";

    private static final String OUTPUT_TOPIC = "stock-price-moving-average";

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1));

        builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()).withTimestampExtractor(new StockPriceTimestampExtractor()))
                .peek((key, value) -> log.info("0. input() - key: {}, value: {}", key, value))
                .groupByKey(Grouped.with("stock-ticker-group-by-timestamp-stream", Serdes.String(), StockPriceSerdes.stockPrice()))
//                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(3)))
//                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(3)))
                .windowedBy(timeWindows)
                .aggregate(() ->
                        new CountAndSum(0L, 0D),
                        (key, value, aggregate) -> {
//                            log.info("1. aggregate() - key: {}, value: {}, aggregate: {}", key, value, aggregate);
                            return new CountAndSum(aggregate.getCount() + 1, aggregate.getSum() + value.getClose());
                        },
                        Named.as("stock-ticker-time-interval-with-count-and-sum-table"),
                        Materialized.with(Serdes.String(), StockPriceSerdes.countAndSum()))
//                .toStream()
                .mapValues((key, value) -> {
//                        log.info("2. mapValues() - time range: [{} - {}], value: {}", key.window().startTime(), key.window().endTime(), value);
                        return value.getSum() / value.getCount();
                    },
                        Named.as("stock-ticker-time-interval-with-average"),
                        Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(
                                String.class, timeWindows.size()),
                                Serdes.Double())
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName("average-time-window-suppressed"))
                .toStream()
                .peek((key, value) -> log.info("3. output: time range: [{} - {}], value: {}", key.window().startTime(), key.window().endTime(), value))
                .map((key, value) -> KeyValue.pair(key.key(), value))
//                .peek((key, value) -> log.info("3. output: key: {}, value: {}", key, value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));

        return builder.build();
    }
}
