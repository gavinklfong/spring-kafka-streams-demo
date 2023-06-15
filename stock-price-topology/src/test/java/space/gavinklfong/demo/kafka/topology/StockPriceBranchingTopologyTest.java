package space.gavinklfong.demo.kafka.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.util.StockPriceSerdes;

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StockPriceBranchingTopologyTest {

    private Topology topology;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, StockPrice> inputTopic;
    private TestOutputTopic<String, StockPrice> techOutputTopic;
    private TestOutputTopic<String, StockPrice> healthOutputTopic;
    private TestOutputTopic<String, StockPrice> othersOutputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();

    @BeforeEach
    void setup() {
        topology = StockPriceBranchingTopology.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-branching");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("stock-price", stringSerde.serializer(), StockPriceSerdes.stockPrice().serializer());
        techOutputTopic = testDriver.createOutputTopic("stock-price-branching-tech", stringSerde.deserializer(), StockPriceSerdes.stockPrice().deserializer());
        healthOutputTopic = testDriver.createOutputTopic("stock-price-branching-health", stringSerde.deserializer(), StockPriceSerdes.stockPrice().deserializer());
        othersOutputTopic = testDriver.createOutputTopic("stock-price-branching-others", stringSerde.deserializer(), StockPriceSerdes.stockPrice().deserializer());

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
        inputTopic.pipeInput("XOM", stockPrice);

        assertThat(techOutputTopic.isEmpty()).isTrue();
        assertThat(healthOutputTopic.isEmpty()).isTrue();
        assertThat(othersOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("XOM", stockPrice));
    }

}
