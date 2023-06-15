package space.gavinklfong.demo.kafka.topology;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import space.gavinklfong.demo.kafka.model.StockCategory;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import static space.gavinklfong.demo.kafka.model.StockCategory.OTHERS;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class StockPriceBranchingTopology {
    private static final String INPUT_TOPIC = "stock-price";
    private static final String OUTPUT_TOPIC = "stock-price-branching";
    private static final Map<StockCategory, Set<String>> STOCK_TICKER_CATEGORY_MAP =
            Map.of(StockCategory.HEALTH, Set.of("UNH"),
                    StockCategory.TECH, Set.of("APPL", "MSFT", "IBM"));

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockPrice> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), StockPriceSerdes.stockPrice()))
                .filterNot((key, value) -> key.equals("IBM"), Named.as("filtered-stock-price-stream"));
        source.peek((key, value) -> log.info("key: {}, value: {}", key, value));
        BranchedKStream<String, StockPrice> branched = source.split(Named.as("stock-category"));
        for (StockCategory category : StockCategory.values()) {
                branched.branch((key, value) -> isBelongToCategory.test(category, key),
                    Branched.withConsumer(ks -> ks.to(String.format("%s-%s", OUTPUT_TOPIC, category.name().toLowerCase()))));
        }

        branched.defaultBranch(Branched.withConsumer(ks -> ks.to(String.format("%s-%s", OUTPUT_TOPIC, OTHERS.name().toLowerCase()))));

        return builder.build();
    }

    private static final BiPredicate<StockCategory, String> isBelongToCategory = (category, ticker) ->
            STOCK_TICKER_CATEGORY_MAP.getOrDefault(category, Collections.emptySet()).contains(ticker);

}
