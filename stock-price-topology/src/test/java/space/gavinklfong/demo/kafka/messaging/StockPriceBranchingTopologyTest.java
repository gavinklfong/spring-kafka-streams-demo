package space.gavinklfong.demo.kafka.messaging;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import space.gavinklfong.demo.kafka.schema.CountAndSum;
import space.gavinklfong.demo.kafka.schema.StockPrice;
import space.gavinklfong.demo.kafka.schema.TickerAndTimestamp;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class StockPriceBranchingTopologyTest {

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
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("stock-price", stringSerde.serializer(), getStockPriceSerde().serializer());
        techOutputTopic = testDriver.createOutputTopic("stock-price-branching-tech", stringSerde.deserializer(), getStockPriceSerde().deserializer());
        healthOutputTopic = testDriver.createOutputTopic("stock-price-branching-health", stringSerde.deserializer(), getStockPriceSerde().deserializer());
        othersOutputTopic = testDriver.createOutputTopic("stock-price-branching-others", stringSerde.deserializer(), getStockPriceSerde().deserializer());

    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void doTest() {
        StockPrice stockPrice = StockPrice.newBuilder()
                .setClose(100).setHigh(999).setLow(111).setOpen(0).setTimestamp(Instant.now()).setVolume(888)
                .build();
        inputTopic.pipeInput("XOM", stockPrice);

        assertThat(techOutputTopic.isEmpty()).isTrue();
        assertThat(healthOutputTopic.isEmpty()).isTrue();
        assertThat(othersOutputTopic.readKeyValue()).isEqualTo(new KeyValue<>("XOM", stockPrice));
    }

    private SpecificAvroSerde<StockPrice> getStockPriceSerde() {
        SpecificAvroSerde<StockPrice> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(), false);
        return serde;
    }

    private Map<String, String> getSerdeConfig() {
        return Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    }
}
