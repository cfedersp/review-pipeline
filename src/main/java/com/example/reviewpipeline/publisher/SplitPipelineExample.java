package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import com.example.reviewpipeline.service.ClientPartitionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * Example demonstrating how ParallelPublisherProcessor splits pipelines by accountId.
 *
 * When items from different accounts are received, they are automatically
 * routed to separate processing pipelines, allowing for account-level isolation.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SplitPipelineExample {

    private final JdbcPollingPublisherFactory publisherFactory;
    private final ReviewQueueRepository reviewQueueRepository;
    private final ClientPartitionManager clientPartitionManager;

    /**
     * Demonstrates pipeline splitting by accountId.
     *
     * For example, if reviews come in with:
     * - Review 1: client-A:account-1:UPDATE
     * - Review 2: client-B:account-1:DELETE
     * - Review 3: client-A:account-2:UPDATE
     *
     * Two separate pipelines will be created:
     * - Pipeline 1: Handles all items with account-1 (Reviews 1 & 2)
     * - Pipeline 2: Handles all items with account-2 (Review 3)
     */
    public void startSplitPipelineProcessing() {
        // Create publisher for reviews
        JdbcPollingPartitionedPublisher<ReviewQueue> publisher =
                publisherFactory.createPublisher(
                        this::getNextReview,
                        clientPartitionManager::tryAcquireClient,
                        clientPartitionManager::releaseClient
                );

        // Create processor that automatically splits by accountId
        ParallelPublisherProcessor<ReviewQueue> processor =
                ParallelPublisherProcessor.<ReviewQueue>builder()
                        .publisher(publisher)
                        .processor(this::processReview)
                        .maxConcurrency(10)
                        .preProcessor(review ->
                                log.info("Routing review {} to pipeline for account: {}",
                                        review.getId(), review.getAccountId()))
                        .successHandler(review ->
                                log.info("Review {} processed in account {} pipeline",
                                        review.getId(), review.getAccountId()))
                        .build();

        processor.startAsync();

        log.info("Split pipeline processor started - items will be routed to separate " +
                "pipelines based on accountId");
    }

    /**
     * Demonstrates using multiple publishers with split pipelines.
     * Items from all publishers are merged and then split by accountId.
     */
    public Flux<ReviewQueue> createMultiSourceSplitPipeline() {
        // Publisher 1: High priority reviews
        Flux<ReviewQueue> highPriorityPublisher =
                publisherFactory.createPublisher(
                        this::getHighPriorityReview,
                        clientPartitionManager::tryAcquireClient,
                        clientPartitionManager::releaseClient,
                        2000L
                ).createPublisher();

        // Publisher 2: Normal reviews
        Flux<ReviewQueue> normalPublisher =
                publisherFactory.createPublisher(
                        this::getNormalReview,
                        clientPartitionManager::tryAcquireClient,
                        clientPartitionManager::releaseClient
                ).createPublisher();

        // Both publishers feed into processor, which splits by accountId
        return ParallelPublisherProcessor.<ReviewQueue>builder()
                .publisher(highPriorityPublisher)
                .publisher(normalPublisher)
                .processor(this::processReview)
                .maxConcurrency(15)
                .preProcessor(review ->
                        log.info("[Account {}] Received review: client={}, operation={}",
                                review.getAccountId(),
                                review.getClientId(),
                                review.getOperation()))
                .build()
                .start();
    }

    /**
     * Processes a review and returns when complete.
     */
    private Mono<Void> processReview(ReviewQueue review) {
        return Mono.fromRunnable(() -> {
            log.info("[Account {}] Processing review {} with partition key: {}",
                    review.getAccountId(),
                    review.getId(),
                    review.getPartitionKey());

            // Simulate processing
            review.setProcessed(true);
            review.setProcessedDate(LocalDateTime.now());
            reviewQueueRepository.save(review);

            log.info("[Account {}] Completed review {}",
                    review.getAccountId(),
                    review.getId());
        });
    }

    private ReviewQueue getNextReview() {
        return reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(null, false)
                .stream()
                .findFirst()
                .orElse(null);
    }

    private ReviewQueue getHighPriorityReview() {
        // Simulated - would filter by priority
        return getNextReview();
    }

    private ReviewQueue getNormalReview() {
        // Simulated - would filter by priority
        return getNextReview();
    }
}
