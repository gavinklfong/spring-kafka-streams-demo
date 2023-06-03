package space.gavinklfong.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.gavinklfong.demo.kafka.config.service.StockPriceTask;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class StockPriceProducerApp implements CommandLineRunner {

    @Autowired
    private List<StockPriceTask> stockPriceTasks;

    public static void main(String[] args) {
        SpringApplication.run(StockPriceProducerApp.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Running executor service");
//        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
//        executorService.scheduleAtFixedRate(ibmStockPriceTask, 1, 1, TimeUnit.SECONDS);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
        stockPriceTasks.forEach(task ->
                executorService.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS));
    }
}
