package employee.consumer;

import employee.consumer.kafka.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
@EnableEurekaClient
@EnableMongoRepositories
public class EmployeeDataReader {

	public static void main(String[] args) throws InterruptedException {

		// MAIN APPLICATION
		SpringApplication.run(EmployeeDataReader.class, args);

		// HAVE THREAD SLEEP FOR 10 SEC
		Thread.sleep(10000);

		// INITIALIZE CONSUMER TO START LISTENING
		Consumer.listen();
	}
}
