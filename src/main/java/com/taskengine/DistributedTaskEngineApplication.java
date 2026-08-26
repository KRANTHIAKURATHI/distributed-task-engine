package com.taskengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedTaskEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(
				DistributedTaskEngineApplication.class,
				args
		);
	}
}