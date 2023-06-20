package space.gavinklfong.demo.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import space.gavinklfong.demo.kafka.model.CountAndSum;
import space.gavinklfong.demo.kafka.model.MedianStockPrice;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.topology.util.TestDataBuilder;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static space.gavinklfong.demo.kafka.topology.util.TestDataBuilder.buildAMAZExpectedMovingAverage;
import static space.gavinklfong.demo.kafka.topology.util.TestDataBuilder.buildUNHExpectedMovingAverage;

@Slf4j
class StockPriceMovingAverageWindowTopologyTest {


    private TopologyTestDriver testDriver;
    private TestInputTopic<String, StockPrice> inputTopic;
    private TestOutputTopic<String, Double> outputTopic;
    private final Serde<String> stringSerde = new Serdes.StringSerde();

    @BeforeEach
    void setup() {
        Topology topology = StockPriceMovingAverageWindowTopology.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-moving-average");

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("stock-price", stringSerde.serializer(), StockPriceSerdes.stockPrice().serializer());
        outputTopic = testDriver.createOutputTopic("stock-price-moving-average", stringSerde.deserializer(), Serdes.Double().deserializer());

    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void verifyAMAZStockPricesMovingAverage() {
        List<StockPrice> stockPrices = TestDataBuilder.buildAMAZSampleStockPrices();
        List<KeyValue<String, Double>> expectedOutputs = buildAMAZExpectedMovingAverage();

        stockPrices.forEach(price -> inputTopic.pipeInput("AMAZ", price));

        List<KeyValue<String, Double>> output = outputTopic.readKeyValuesToList();
        List<KeyValue<String, Double>> amazMovingAverages = output.stream().filter(entry -> "AMAZ".equals(entry.key)).toList();
        List<KeyValue<String, Double>> unhMovingAverages = output.stream().filter(entry -> "UNH".equals(entry.key)).toList();


        assertThat(amazMovingAverages).isEqualTo(expectedOutputs);

//        log.info("output: {}", outputTopic.readValuesToList());
//        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("APPL", medianStockPrice));
    }

    @Test
    void doTest() {
        List<KeyValue<String, StockPrice>> stockPrices = TestDataBuilder.buildSampleStockPrices();
        List<KeyValue<String, Double>> amazExpectedOutputs = buildAMAZExpectedMovingAverage();
        List<KeyValue<String, Double>> unhExpectedOutputs = buildUNHExpectedMovingAverage();

        stockPrices.forEach(stockPrice -> {
            log.info("{}", stockPrice);
            inputTopic.pipeInput(stockPrice.key, stockPrice.value);
        });

        List<KeyValue<String, Double>> output = outputTopic.readKeyValuesToList();
        List<KeyValue<String, Double>> amazMovingAverages = output.stream().filter(entry -> "AMAZ".equals(entry.key)).toList();
        List<KeyValue<String, Double>> unhMovingAverages = output.stream().filter(entry -> "UNH".equals(entry.key)).toList();

        assertThat(amazMovingAverages).isEqualTo(amazExpectedOutputs);
        assertThat(unhMovingAverages).isEqualTo(unhExpectedOutputs);
    }

}
