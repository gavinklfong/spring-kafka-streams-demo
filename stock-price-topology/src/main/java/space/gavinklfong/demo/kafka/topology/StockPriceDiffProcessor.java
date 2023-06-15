package space.gavinklfong.demo.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import space.gavinklfong.demo.kafka.model.StockPrice;

import static java.util.Objects.nonNull;

@Slf4j
public class StockPriceDiffProcessor implements Processor<String, StockPrice, String, StockPrice> {

    private KeyValueStore<String, StockPrice> previousStockPriceStore;

    private ProcessorContext<String, StockPrice> processorContext;

    @Override
    public void init(ProcessorContext<String, StockPrice> processorContext) {
        this.previousStockPriceStore = processorContext.getStateStore("previous-stock-price-store");
        this.processorContext = processorContext;
    }

    @Override
    public void process(Record<String, StockPrice> record) {
        StockPrice previousStockPrice = previousStockPriceStore.get(record.key());

        StockPrice stockPrice = record.value();

        StockPrice.StockPriceBuilder stockPriceDiffBuilder = record.value().toBuilder();

        if (nonNull(previousStockPrice)) {
            stockPriceDiffBuilder.low(stockPrice.getLow() - previousStockPrice.getLow());
            stockPriceDiffBuilder.high(stockPrice.getHigh() - previousStockPrice.getHigh());
            stockPriceDiffBuilder.open(stockPrice.getOpen() - previousStockPrice.getOpen());
            stockPriceDiffBuilder.close(stockPrice.getClose() - previousStockPrice.getClose());
            stockPriceDiffBuilder.volume(stockPrice.getVolume() - previousStockPrice.getVolume());
        } else {
            stockPriceDiffBuilder.low(0D).high(0D).open(0D).close(0D).volume(0L);
        }

        previousStockPriceStore.put(record.key(), record.value());

        log.info("StockPriceDiffProcessor output = {}", stockPriceDiffBuilder.build());
        processorContext.forward(record.withValue(stockPriceDiffBuilder.build()));
    }

}
