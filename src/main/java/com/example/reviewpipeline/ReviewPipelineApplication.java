package com.example.reviewpipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class ReviewPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReviewPipelineApplication.class, args);
    }
}
