package com.taskengine;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DistributedTaskEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(DistributedTaskEngineApplication.class, args);
	}

}
