package space.gavinklfong.demo.kafka.topology;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

@Slf4j
class PocTest {

    @Test
    void doRound() {
        double d = 12.2526;
        log.info("{}", Math.scalb(d, 2));
    }

    @Test
    void doTest2() {
        convertNumber(2);
    }

    @Test
    void doTest5() {
        convertNumber(5);
    }

    @Test
    void doTest10() {
        convertNumber(10);
    }

    @Test
    void doTest30() {
        convertNumber(30);
    }

    private void convertNumber(int interval) {
        IntStream.rangeClosed(0, 59)
                .forEach(value -> log.info("{} -> {}", value,
                                (value - (value % interval) + interval)
                        )
                );
    }


}
