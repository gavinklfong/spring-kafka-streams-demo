package space.gavinklfong.demo.kafka.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import space.gavinklfong.demo.kafka.model.*;

import java.math.BigDecimal;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StockPriceSerdes {

    public static JsonSerde<StockPrice> stockPrice() {
        return stockPrice(false);
    }

    public static JsonSerde<StockPrice> stockPrice(boolean isKey) {
        JsonSerde<StockPrice> serde = new JsonSerde<>(StockPrice.class);
        serde.configure(getSerDeConfig(), isKey);
        return serde;
    }

    public static JsonSerde<MedianStockPrice> medianStockPrice() {
        JsonSerde<MedianStockPrice> serde = new JsonSerde<>(MedianStockPrice.class);
        serde.configure(getSerDeConfig(), false);
        return serde;
    }

    public static JsonSerde<TickerAndTimestamp> tickerAndTimestamp() {
        return tickerAndTimestamp(true);
    }

    public static JsonSerde<TickerAndTimestamp> tickerAndTimestamp(boolean isKey) {
        JsonSerde<TickerAndTimestamp> serde = new JsonSerde<>(TickerAndTimestamp.class);
        serde.configure(getSerDeConfig(), isKey);
        return serde;
    }

    public static JsonSerde<CountAndSum> countAndSum() {
        return countAndSum(false);
    }

    public static JsonSerde<CountAndSum> countAndSum(boolean isKey) {
        JsonSerde<CountAndSum> serde = new JsonSerde<>(CountAndSum.class);
        serde.configure(getSerDeConfig(), isKey);
        return serde;
    }

    public static JsonSerde<CountAndRelativeStrength> countAndRelativeStrength() {
        return countAndRelativeStrength(false);
    }

    public static JsonSerde<CountAndRelativeStrength> countAndRelativeStrength(boolean isKey) {
        JsonSerde<CountAndRelativeStrength> serde = new JsonSerde<>(CountAndRelativeStrength.class);
        serde.configure(getSerDeConfig(), isKey);
        return serde;
    }

    private static Map<String, String> getSerDeConfig() {
        return Map.of(JsonDeserializer.TRUSTED_PACKAGES, "space.gavinklfong.demo.kafka.model");
    }
}
