package space.gavinklfong.demo.kafka.messaging;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import space.gavinklfong.demo.kafka.schema.CountAndSum;
import space.gavinklfong.demo.kafka.schema.TickerAndTimestamp;
import space.gavinklfong.demo.kafka.schema.StockPrice;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

@Slf4j
public class StockPriceTopology {

    private static final int MINUTE_INTERVAL = 10;
    private static final String INPUT_TOPIC = "stock-price";

    private static final String OUTPUT_TOPIC = "stock-price-output";

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockPrice> source = builder.stream(INPUT_TOPIC);
//        source.peek((key, value) -> log.info("key: {}, value: {}", key, value));

        KStream<TickerAndTimestamp, StockPrice> sourceInTickerAndTimestampInterval =
                source.selectKey((key, value) -> new TickerAndTimestamp(key, convertTimestampByInterval(value.getTimestamp(), MINUTE_INTERVAL)));

        KGroupedStream<TickerAndTimestamp, StockPrice> sourceGroupedByTickerAndTimestampInterval = sourceInTickerAndTimestampInterval.groupByKey(
                Grouped.with(getTickerAndTimestampSerde(), getStockPriceSerde())
        );

        KTable<TickerAndTimestamp, CountAndSum> countAndSumKTable = sourceGroupedByTickerAndTimestampInterval.aggregate(() ->
                new CountAndSum(0L, 0D),
                (key, value, aggregate) -> {
                    aggregate.setCount(aggregate.getCount() + 1);
                    aggregate.setSum(aggregate.getSum() + value.getClose());
                    return aggregate;
                },
                Materialized.with(getTickerAndTimestampSerde(), getCountAndSumSerde()));

        KTable<TickerAndTimestamp, Double> averageKTable = countAndSumKTable.mapValues(value -> value.getSum() / value.getCount(),
                Materialized.with(getTickerAndTimestampSerde(), Serdes.Double()));
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

    private static SpecificAvroSerde<StockPrice> getStockPriceSerde() {
        SpecificAvroSerde<StockPrice> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(), false);
        return serde;
    }

    private static SpecificAvroSerde<TickerAndTimestamp> getTickerAndTimestampSerde() {
        SpecificAvroSerde<TickerAndTimestamp> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(), true);
        return serde;
    }

    private static SpecificAvroSerde<CountAndSum> getCountAndSumSerde() {
        SpecificAvroSerde<CountAndSum> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(), false);
        return serde;
    }

    private static Map<String, String> getSerdeConfig() {
        return Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    }
}
