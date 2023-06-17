package space.gavinklfong.demo.kafka.topology.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import space.gavinklfong.demo.kafka.model.StockPrice;

public class StockPriceTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {

        StockPrice stockPrice = (StockPrice) consumerRecord.value();

        return stockPrice.getTimestamp().toEpochMilli();
    }
}
