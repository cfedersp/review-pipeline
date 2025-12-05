package com.example.reviewpipeline.service;

import com.example.reviewpipeline.handler.ReviewHandlerRegistry;
import com.example.reviewpipeline.model.KafkaReviewMessage;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Semaphore;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final ReviewHandlerRegistry handlerRegistry;
    private final ClientPartitionManager clientPartitionManager;

    @Value("${review.pipeline.max-concurrent-jobs:10}")
    private int maxConcurrentJobs;

    private Semaphore jobSemaphore;

    @PostConstruct
    public void init() {
        jobSemaphore = new Semaphore(maxConcurrentJobs);
        log.info("Kafka consumer service initialized with max concurrent jobs: {}", maxConcurrentJobs);
    }

    @KafkaListener(
        topics = "${review.pipeline.kafka.topic}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeReviewMessage(KafkaReviewMessage message) {
        log.debug("Received Kafka message for client: {}, type: {}",
            message.getClientFk(), message.getReviewType());

        if (!clientPartitionManager.tryAcquireClient(message.getClientFk())) {
            log.warn("Client {} is already being processed, skipping message", message.getClientFk());
            return;
        }

        processMessage(message)
            .doFinally(signalType -> clientPartitionManager.releaseClient(message.getClientFk()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                result -> log.debug("Successfully processed Kafka message"),
                error -> log.error("Error processing Kafka message", error)
            );
    }

    private Mono<Void> processMessage(KafkaReviewMessage message) {
        return Mono.fromCallable(() -> {
            jobSemaphore.acquire();
            return message;
        })
        .flatMap(msg -> handlerRegistry.handleReview(
            msg.getReviewType(),
            msg.getReviewMessage(),
            msg.getClientFk()
        ))
        .doFinally(signalType -> jobSemaphore.release())
        .onErrorResume(error -> {
            log.error("Error processing Kafka message for client: {}", message.getClientFk(), error);
            return Mono.empty();
        });
    }
}
