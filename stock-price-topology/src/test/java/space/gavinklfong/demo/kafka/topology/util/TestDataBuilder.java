package space.gavinklfong.demo.kafka.topology.util;

import org.apache.kafka.streams.KeyValue;
import space.gavinklfong.demo.kafka.model.StockPrice;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TestDataBuilder {

    public static List<StockPrice> buildAMAZSampleStockPrices() {
        List<StockPrice> stockPrices = new ArrayList<>();
        stockPrices.add(buildStockPrice("2023-05-05 09:00:00", 104.77));
        stockPrices.add(buildStockPrice("2023-05-05 09:01:00", 100));
        stockPrices.add(buildStockPrice("2023-05-05 09:02:00", 90));
        stockPrices.add(buildStockPrice("2023-05-05 09:13:00", 104));
        stockPrices.add(buildStockPrice("2023-05-05 09:14:00", 111));
        stockPrices.add(buildStockPrice("2023-05-05 09:15:00", 115));
        stockPrices.add(buildStockPrice("2023-05-05 09:16:00", 140));
        stockPrices.add(buildStockPrice("2023-05-05 09:17:00", 155));
        stockPrices.add(buildStockPrice("2023-05-05 09:20:00", 160));
        stockPrices.add(buildStockPrice("2023-05-05 09:21:00", 165));
        stockPrices.add(buildStockPrice("2023-05-05 09:25:00", 155));
        stockPrices.add(buildStockPrice("2023-05-05 09:26:00", 130));
        stockPrices.add(buildStockPrice("2023-05-05 09:29:00", 125));
        stockPrices.add(buildStockPrice("2023-05-05 09:30:00", 112));

        return stockPrices;
    }

    public static List<StockPrice> buildUNHSampleStockPrices() {
        List<StockPrice> stockPrices = new ArrayList<>();
        stockPrices.add(buildStockPrice("2023-05-05 09:00:00", 488));
        stockPrices.add(buildStockPrice("2023-05-05 09:01:00", 500));
        stockPrices.add(buildStockPrice("2023-05-05 09:02:00", 540));
        stockPrices.add(buildStockPrice("2023-05-05 09:13:00", 430));
        stockPrices.add(buildStockPrice("2023-05-05 09:14:00", 400));
        stockPrices.add(buildStockPrice("2023-05-05 09:15:00", 320));
        stockPrices.add(buildStockPrice("2023-05-05 09:16:00", 310));
        stockPrices.add(buildStockPrice("2023-05-05 09:17:00", 450));
        stockPrices.add(buildStockPrice("2023-05-05 09:20:00", 500));
        stockPrices.add(buildStockPrice("2023-05-05 09:21:00", 710));
        stockPrices.add(buildStockPrice("2023-05-05 09:25:00", 590));
        stockPrices.add(buildStockPrice("2023-05-05 09:26:00", 435));
        stockPrices.add(buildStockPrice("2023-05-05 09:29:00", 510));
        stockPrices.add(buildStockPrice("2023-05-05 09:30:00", 550));


        return stockPrices;
    }


    public static List<KeyValue<String, StockPrice>> buildSampleStockPrices() {
        List<KeyValue<String, StockPrice>> stockPrices = new ArrayList<>(buildAMAZSampleStockPrices().stream()
                .map(item -> KeyValue.pair("AMAZ", item))
                .toList());

        List<KeyValue<String, StockPrice>> unhStockPrices = buildUNHSampleStockPrices().stream()
                .map(item -> KeyValue.pair("UNH", item))
                .toList();

        stockPrices.addAll(unhStockPrices);
        stockPrices.sort(Comparator.comparing(item -> item.value.getTimestamp()));
        return stockPrices;
    }

    public static List<KeyValue<String, Double>> buildAMAZExpectedMovingAverage() {
        List<KeyValue<String, Double>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(buildKeyValue("AMAZ", 104.77));
        expectedOutputs.add(buildKeyValue("AMAZ", 102.39));
        expectedOutputs.add(buildKeyValue("AMAZ", 98.26));
        expectedOutputs.add(buildKeyValue("AMAZ", 99.69));
        expectedOutputs.add(buildKeyValue("AMAZ", 101.95));
        expectedOutputs.add(buildKeyValue("AMAZ", 104.13));
        expectedOutputs.add(buildKeyValue("AMAZ", 110));
        expectedOutputs.add(buildKeyValue("AMAZ", 119.17));
        expectedOutputs.add(buildKeyValue("AMAZ", 130.83));
        expectedOutputs.add(buildKeyValue("AMAZ", 135.71));
        expectedOutputs.add(buildKeyValue("AMAZ", 138.13));
        expectedOutputs.add(buildKeyValue("AMAZ", 137.22));
        expectedOutputs.add(buildKeyValue("AMAZ", 139.56));
        expectedOutputs.add(buildKeyValue("AMAZ", 139.67));

        return expectedOutputs;
    }

    public static List<KeyValue<String, Double>> buildUNHExpectedMovingAverage() {
        List<KeyValue<String, Double>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(buildKeyValue("UNH", 488));
        expectedOutputs.add(buildKeyValue("UNH", 494));
        expectedOutputs.add(buildKeyValue("UNH", 509.33));
        expectedOutputs.add(buildKeyValue("UNH", 489.5));
        expectedOutputs.add(buildKeyValue("UNH", 471.6));
        expectedOutputs.add(buildKeyValue("UNH", 446.33));
        expectedOutputs.add(buildKeyValue("UNH", 416.67));
        expectedOutputs.add(buildKeyValue("UNH", 408.33));
        expectedOutputs.add(buildKeyValue("UNH", 401.67));
        expectedOutputs.add(buildKeyValue("UNH", 445.71));
        expectedOutputs.add(buildKeyValue("UNH", 463.75));
        expectedOutputs.add(buildKeyValue("UNH", 460.56));
        expectedOutputs.add(buildKeyValue("UNH", 469.44));
        expectedOutputs.add(buildKeyValue("UNH", 486.11));

        return expectedOutputs;
    }

    private static KeyValue<String, Double> buildKeyValue(String key, double value) {
        return KeyValue.pair(key, value);
    }


    private static StockPrice buildStockPrice(String timestamp, double price) {
        Instant timestampInstant = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(ZoneId.of("GMT"))
                .toInstant();
        return StockPrice.builder().timestamp(timestampInstant).close(price).build();
    }
}
