package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import space.gavinklfong.demo.kafka.model.CountAndSum;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.model.TickerAndTimestamp;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;
//import space.gavinklfong.demo.kafka.schema.CountAndSum;
//import space.gavinklfong.demo.kafka.schema.TickerAndTimestamp;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StockPriceMovingAverageTopology {

    private static final int MINUTE_INTERVAL = 10;
    private static final String INPUT_TOPIC = "stock-price";

    private static final String OUTPUT_TOPIC = "stock-price-output";

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockPrice> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()));
//        source.peek((key, value) -> log.info("key: {}, value: {}", key, value));

        KStream<TickerAndTimestamp, StockPrice> sourceInTickerAndTimestampInterval =
                source.selectKey((key, value) -> new TickerAndTimestamp(key, convertTimestampByInterval(value.getTimestamp(), MINUTE_INTERVAL)),
                        Named.as("stock-price-timestamp-key-stream"));

        KGroupedStream<TickerAndTimestamp, StockPrice> sourceGroupedByTickerAndTimestampInterval = sourceInTickerAndTimestampInterval.groupByKey(
                Grouped.with("stock-ticker-group-by-timestamp-stream", StockPriceSerdes.tickerAndTimestamp(), StockPriceSerdes.stockPrice())
        );

        KTable<TickerAndTimestamp, CountAndSum> countAndSumKTable = sourceGroupedByTickerAndTimestampInterval.aggregate(() ->
                new CountAndSum(0L, BigDecimal.ZERO),
                (key, value, aggregate) -> (
                   new CountAndSum(aggregate.getCount() + 1, aggregate.getSum().add(BigDecimal.valueOf(value.getClose())))
                ),
                Named.as("stock-ticker-time-interval-with-count-and-sum-table"),
                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), StockPriceSerdes.countAndSum()));

        KTable<TickerAndTimestamp, Double> averageKTable = countAndSumKTable.mapValues(value -> value.getSum()
                        .divide(BigDecimal.valueOf(value.getCount()), RoundingMode.HALF_UP).doubleValue(),
                Named.as("stock-ticker-time-interval-with-average"),
                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), Serdes.Double()));

        averageKTable.toStream()
                .peek((key, value) -> log.info("key: {}, value: {}", key, value))
                .to(OUTPUT_TOPIC);

        return builder.build();
    }

    private static Instant convertTimestampByInterval(Instant timestamp, int intervalInMinutes) {
        ZonedDateTime zonedDateTime = timestamp.atZone(ZoneId.systemDefault());
        ZonedDateTime timestampWithInterval = zonedDateTime.truncatedTo(ChronoUnit.HOURS);
        int timestampMinuteInHour = zonedDateTime.getMinute();
        int minuteWithInterval = timestampMinuteInHour - (timestampMinuteInHour % intervalInMinutes) + intervalInMinutes;
        if (minuteWithInterval == 60) {
            return timestampWithInterval.plusHours(1).toInstant();
        } else {
            return timestampWithInterval.withMinute(minuteWithInterval).toInstant();
        }
    }
}
