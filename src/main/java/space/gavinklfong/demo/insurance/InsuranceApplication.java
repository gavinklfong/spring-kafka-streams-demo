package space.gavinklfong.demo.insurance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.gavinklfong.demo.insurance.messaging.WordCountTopology;

@Slf4j
@SpringBootApplication
public class InsuranceApplication  {

	public static void main(String[] args) {
		SpringApplication.run(InsuranceApplication.class, args);
	}


}
