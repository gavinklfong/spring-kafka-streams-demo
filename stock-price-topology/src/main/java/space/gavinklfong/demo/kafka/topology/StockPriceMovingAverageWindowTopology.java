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

import java.math.BigDecimal;
import java.math.RoundingMode;
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

    private static final String INPUT_TOPIC = "stock-price";
    private static final String OUTPUT_TOPIC = "stock-price-moving-average";
    private static final SlidingWindows TIME_WINDOWS = SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(15));

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()).withTimestampExtractor(new StockPriceTimestampExtractor()))
                .groupByKey(Grouped.with("stock-ticker-group-by-timestamp-stream", Serdes.String(), StockPriceSerdes.stockPrice()))
                .windowedBy(TIME_WINDOWS)
                .aggregate(() ->
                        new CountAndSum(0L, BigDecimal.ZERO),
                        (key, value, aggregate) -> new CountAndSum(aggregate.getCount() + 1, aggregate.getSum().add(BigDecimal.valueOf(value.getClose()))),
                        Named.as("stock-ticker-time-interval-with-count-and-sum-table"),
                        Materialized.with(Serdes.String(), StockPriceSerdes.countAndSum()))
                .mapValues((key, value) -> value.getSum()
                                .divide(BigDecimal.valueOf(value.getCount()), 2, RoundingMode.HALF_UP)
                                .doubleValue(),
                        Named.as("stock-ticker-time-interval-with-average"),
                        Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(
                                String.class, TIME_WINDOWS.timeDifferenceMs()),
                                Serdes.Double())
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName("average-time-window-suppressed"))
                .toStream()
                .filter((key, value) -> key.window().start() % 1000 == 0)
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));

        return builder.build();
    }
}
