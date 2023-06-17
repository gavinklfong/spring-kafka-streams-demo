package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import space.gavinklfong.demo.kafka.model.CountAndRelativeStrength;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.model.TickerAndTimestamp;
import space.gavinklfong.demo.kafka.topology.processor.StockPriceDiffProcessor;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StockPriceRSITopology {

    private static final String STATE_STORE = "previous-stock-price-store";
    private static final int MINUTE_INTERVAL = 10;
    private static final String INPUT_TOPIC = "stock-price";

    private static final String OUTPUT_TOPIC = "stock-price-10m-rsi";

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, StockPrice>> previousStockPriceStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE),
                Serdes.String(),
                StockPriceSerdes.stockPrice()
        );
        builder.addStateStore(previousStockPriceStoreBuilder);


        KStream<String, StockPrice> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()));
        source.process(StockPriceDiffProcessor::new, Named.as("stock-price-diff-transformer"), STATE_STORE)
                .selectKey((key, value) -> new TickerAndTimestamp(key, convertTimestampByInterval(value.getTimestamp(), MINUTE_INTERVAL)),
                        Named.as("stock-price-timestamp-key-stream"))
                .groupByKey(Grouped.with("stock-ticker-group-by-timestamp-stream", StockPriceSerdes.tickerAndTimestamp(),
                        StockPriceSerdes.stockPrice()))
                .aggregate(() ->
                        new CountAndRelativeStrength(0L, 0D, 0D),
                (key, value, aggregate) -> {
                    log.info("aggregate input: value = {}, aggregate = {}", value, aggregate);
                    CountAndRelativeStrength result;
                    if (value.getClose() == 0)
                        result = aggregate.withCount(aggregate.getCount() + 1);
                    else if (value.getClose() > 0)
                        result = aggregate.withCount(aggregate.getCount() + 1).withTotalGain(aggregate.getTotalGain() + value.getClose());
                    else
                        result = aggregate.withCount(aggregate.getCount() + 1).withTotalLoss(aggregate.getTotalLoss() + value.getClose());

                    log.info("aggregate output = {}", result);
                    return result;
                },
                Named.as("stock-ticker-time-interval-with-count-and-rs-table"),
                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), StockPriceSerdes.countAndRelativeStrength()))
                .mapValues(value -> {
                    log.info("RSI input = {}", value);
                    return 100 / (1 + ((value.getTotalGain() / value.getCount()) / (value.getTotalLoss() * (-1) / value.getCount())));
                },
                Named.as("stock-ticker-time-interval-with-rsi"),
                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), Serdes.Double()))
                .toStream()
                .peek((key, value) -> log.info("key: {}, value: {}", key, value))
                .to(OUTPUT_TOPIC);

//        KStream<TickerAndTimestamp, StockPrice> sourceInTickerAndTimestampInterval =
//                source.selectKey((key, value) -> new TickerAndTimestamp(key, convertTimestampByInterval(value.getTimestamp(), MINUTE_INTERVAL)),
//                        Named.as("stock-price-timestamp-key-stream"));
//
//        KGroupedStream<TickerAndTimestamp, StockPrice> sourceGroupedByTickerAndTimestampInterval = sourceInTickerAndTimestampInterval.groupByKey(
//                Grouped.with("stock-ticker-group-by-timestamp-stream", StockPriceSerdes.tickerAndTimestamp(), StockPriceSerdes.stockPrice())
//        );

//        KTable<TickerAndTimestamp, CountAndRelativeStrength> countAndRSKTable = sourceGroupedByTickerAndTimestampInterval.aggregate(() ->
//                new CountAndRelativeStrength(0L, 0D, 0D),
//                (key, value, aggregate) -> {
//                    log.info("aggregate input: value = {}, aggregate = {}", value, aggregate);
//                    CountAndRelativeStrength result;
//                    if (value.getClose() == 0)
//                        result = aggregate.withCount(aggregate.getCount() + 1);
//                    else if (value.getClose() > 0)
//                        result = aggregate.withCount(aggregate.getCount() + 1).withTotalGain(aggregate.getTotalGain() + value.getClose());
//                    else
//                        result = aggregate.withCount(aggregate.getCount() + 1).withTotalLoss(aggregate.getTotalLoss() + value.getClose());
//
//                    log.info("aggregate output = {}", result);
//                    return result;
//                },
//                Named.as("stock-ticker-time-interval-with-count-and-rs-table"),
//                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), StockPriceSerdes.countAndRelativeStrength()));

//        KTable<TickerAndTimestamp, Double> rsiKTable = countAndRSKTable.mapValues(value -> {
//                    log.info("RSI input = {}", value);
//                   return 100 / (1 + ((value.getTotalGain() / value.getCount()) / (value.getTotalLoss() / value.getCount())));
//                },
//                Named.as("stock-ticker-time-interval-with-rsi"),
//                Materialized.with(StockPriceSerdes.tickerAndTimestamp(), Serdes.Double()));
//
//        rsiKTable.toStream()
//                .peek((key, value) -> log.info("key: {}, value: {}", key, value))
//                .to(OUTPUT_TOPIC);

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
