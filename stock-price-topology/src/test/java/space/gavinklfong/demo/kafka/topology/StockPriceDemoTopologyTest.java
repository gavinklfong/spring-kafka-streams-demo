package space.gavinklfong.demo.kafka.topology;

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
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StockPriceDemoTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, StockPrice> inputTopic;
    private TestOutputTopic<String, MedianStockPrice> outputTopic;
    private final Serde<String> stringSerde = new Serdes.StringSerde();

    @BeforeEach
    void setup() {
        Topology topology = StockPriceDemoTopology.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-demo");

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("stock-price", stringSerde.serializer(), StockPriceSerdes.stockPrice().serializer());
        outputTopic = testDriver.createOutputTopic("transformed-stock-price", stringSerde.deserializer(), StockPriceSerdes.medianStockPrice().deserializer());

    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void doTest() {
        StockPrice stockPrice = StockPrice.builder()
                .close(100D).high(999D).low(111D).open(0D).timestamp(Instant.now()).volume(888L)
                .build();

        MedianStockPrice medianStockPrice = MedianStockPrice.builder()
                .timestamp(stockPrice.getTimestamp())
                .volume(stockPrice.getVolume())
                .median((stockPrice.getHigh() - stockPrice.getLow()) / 2)
                .build();

        inputTopic.pipeInput("APPL", stockPrice);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("APPL", medianStockPrice));
    }

}
