package hello.springbatch5;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Springbatch5Application {

	public static void main(String[] args) {
		SpringApplication.run(Springbatch5Application.class, args);
	}

}
