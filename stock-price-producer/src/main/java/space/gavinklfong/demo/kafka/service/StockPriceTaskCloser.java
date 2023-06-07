package space.gavinklfong.demo.kafka.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@RequiredArgsConstructor
@Component
public class StockPriceTaskCloser implements Runnable {

    private final List<StockPriceTask> stockPriceTasks;

    @Override
    public void run() {
        stockPriceTasks.forEach(StockPriceTask::close);
    }
}
