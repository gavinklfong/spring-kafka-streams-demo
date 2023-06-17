package space.gavinklfong.demo.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import space.gavinklfong.demo.kafka.model.MedianStockPrice;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-demo");
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("stock-price", stringSerde.serializer(), StockPriceSerdes.stockPrice().serializer());
        outputTopic = testDriver.createOutputTopic("stock-price-moving-average", stringSerde.deserializer(), Serdes.Double().deserializer());

    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void doTest() {
        Instant timestamp = Instant.parse("2023-04-01T09:00:00Z");
        double stockPrice = 100D;

        do {
            inputTopic.pipeInput("APPL", stockPrice(timestamp, stockPrice));
            timestamp = timestamp.plus(1, ChronoUnit.MINUTES);
            stockPrice--;
        } while (timestamp.isBefore(Instant.parse("2023-04-01T09:30:00Z")));


//        log.info("output: {}", outputTopic.readValuesToList());
//        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("APPL", medianStockPrice));
    }

    private StockPrice stockPrice(Instant timestamp, double closePrice) {
        return StockPrice.builder()
                .timestamp(timestamp)
                .close(closePrice).high(999D).low(111D).open(0D).volume(888L)
                .build();
    }
}
