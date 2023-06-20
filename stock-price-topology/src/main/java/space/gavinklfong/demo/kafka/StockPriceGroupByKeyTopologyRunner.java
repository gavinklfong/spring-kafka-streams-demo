package space.gavinklfong.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import space.gavinklfong.demo.kafka.topology.StockPriceDemoTopology;
import space.gavinklfong.demo.kafka.topology.StockPriceGroupByKeyTopology;

import java.util.Properties;

@Slf4j
public class StockPriceGroupByKeyTopologyRunner {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-price-group-by-ticker");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Topology topology = StockPriceGroupByKeyTopology.build();
        log.info("{}", topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
