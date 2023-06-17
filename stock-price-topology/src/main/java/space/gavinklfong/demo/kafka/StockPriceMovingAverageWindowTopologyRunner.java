package space.gavinklfong.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import space.gavinklfong.demo.kafka.topology.StockPriceMovingAverageTopology;
import space.gavinklfong.demo.kafka.topology.StockPriceMovingAverageWindowTopology;

import java.util.Properties;

@Slf4j
public class StockPriceMovingAverageWindowTopologyRunner {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-moving-average-window");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Topology topology = StockPriceMovingAverageWindowTopology.build();
        log.info("Topology: {}", topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }
}
