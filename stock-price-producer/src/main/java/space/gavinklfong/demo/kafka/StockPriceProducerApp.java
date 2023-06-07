package space.gavinklfong.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.gavinklfong.demo.kafka.config.AppProperties;
import space.gavinklfong.demo.kafka.service.StockPriceTask;
import space.gavinklfong.demo.kafka.service.StockPriceTaskCloser;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class StockPriceProducerApp implements CommandLineRunner {

    private Random rand = SecureRandom.getInstanceStrong();

    @Autowired
    private List<StockPriceTask> stockPriceTasks;

    @Autowired
    private AppProperties appProperties;

    @Autowired
    private StockPriceTaskCloser stockPriceTaskCloser;

    public StockPriceProducerApp() throws NoSuchAlgorithmException {
    }

    public static void main(String[] args) {
        SpringApplication.run(StockPriceProducerApp.class, args);
    }

    @Override
    public void run(String... args) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(appProperties.getThreadPoolSize());
        stockPriceTasks.forEach(task ->
                executorService.scheduleAtFixedRate(task,
                        getInitialDelay(), appProperties.getPeriodMs(), TimeUnit.MILLISECONDS));

        Runtime.getRuntime().addShutdownHook(new Thread(stockPriceTaskCloser));
    }

    private long getInitialDelay() {
        return rand.nextLong(1001) + 500;
    }
}
