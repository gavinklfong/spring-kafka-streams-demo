package space.gavinklfong.demo.kafka.topology.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import space.gavinklfong.demo.kafka.model.StockPrice;
import space.gavinklfong.demo.kafka.topology.StockPriceDiffProcessor;

public class StockPriceDiffProcessorSupplier implements ProcessorSupplier<String, StockPrice, String, StockPrice> {
    public Processor<String, StockPrice, String, StockPrice> get() {
        return new StockPriceDiffProcessor();
    }
}
