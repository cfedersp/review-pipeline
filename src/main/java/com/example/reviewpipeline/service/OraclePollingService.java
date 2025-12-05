package com.example.reviewpipeline.service;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.handler.ReviewHandlerRegistry;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Semaphore;

@Service
@RequiredArgsConstructor
@Slf4j
public class OraclePollingService {

    private final ReviewQueueRepository reviewQueueRepository;
    private final ReviewHandlerRegistry handlerRegistry;
    private final ClientPartitionManager clientPartitionManager;

    @Value("${review.pipeline.max-concurrent-jobs:10}")
    private int maxConcurrentJobs;

    private Semaphore jobSemaphore;

    @PostConstruct
    public void init() {
        jobSemaphore = new Semaphore(maxConcurrentJobs);
        log.info("Oracle polling service initialized with max concurrent jobs: {}", maxConcurrentJobs);
    }

    @Scheduled(fixedDelayString = "${review.pipeline.oracle.poll-interval-ms:5000}")
    public void pollReviewQueue() {
        log.debug("Polling Oracle REVIEW_QUEUE for unprocessed items");

        List<String> clientFks = reviewQueueRepository.findDistinctUnprocessedClientFks();

        Flux.fromIterable(clientFks)
            .filter(clientFk -> clientPartitionManager.tryAcquireClient(clientFk))
            .flatMap(clientFk -> processClientReviews(clientFk)
                .doFinally(signalType -> clientPartitionManager.releaseClient(clientFk)))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                result -> log.debug("Processed review from Oracle"),
                error -> log.error("Error processing Oracle reviews", error)
            );
    }

    private Mono<Void> processClientReviews(String clientFk) {
        return Mono.fromCallable(() -> reviewQueueRepository.findUnprocessedByClientFk(clientFk))
            .flatMapMany(Flux::fromIterable)
            .flatMap(this::processReview)
            .then();
    }

    private Mono<Void> processReview(ReviewQueue review) {
        return Mono.fromCallable(() -> {
            jobSemaphore.acquire();
            return review;
        })
        .flatMap(r -> handlerRegistry.handleReview(
            r.getReviewType(),
            r.getReviewMessage(),
            r.getClientFk()
        ))
        .then(Mono.fromRunnable(() -> markAsProcessed(review)))
        .doFinally(signalType -> jobSemaphore.release())
        .onErrorResume(error -> {
            log.error("Error processing review ID: {}", review.getId(), error);
            return Mono.empty();
        });
    }

    private void markAsProcessed(ReviewQueue review) {
        review.setProcessed(true);
        review.setProcessedDate(LocalDateTime.now());
        reviewQueueRepository.save(review);
        log.info("Marked review ID {} as processed", review.getId());
    }
}
