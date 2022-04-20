package com.example.demo;

import com.example.demo.service.ConsumerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CqrsConsumerApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(CqrsConsumerApplication.class, args);

	}

}
