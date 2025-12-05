package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import com.example.reviewpipeline.service.ClientPartitionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * Example demonstrating usage of JdbcPollingPartitionedPublisher
 * with entities that implement the Partitionable interface.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PartitionedPublisherUsageExample {

    private final ReviewQueueRepository reviewQueueRepository;
    private final ClientPartitionManager clientPartitionManager;

    /**
     * Creates a partitioned publisher for ReviewQueue items.
     * The publisher automatically extracts partition keys from ReviewQueue.getPartitionKey()
     * (which returns the clientFk).
     *
     * @return A Flux of ReviewQueue items with automatic partitioning
     */
    public Flux<ReviewQueue> createPartitionedPublisher() {
        return JdbcPollingPartitionedPublisher.<ReviewQueue>builder()
                // Polling function - retrieves unprocessed reviews
                .pollFunction(() ->
                    reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(null, false))

                // Partition lock management
                .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                .partitionReleaser(clientPartitionManager::releaseClient)

                // Configuration
                .pollIntervalSeconds(5)
                .batchProcessor(batch ->
                    log.info("Polled {} unprocessed reviews", batch.size()))
                .itemFilter(review -> !review.getProcessed())
                .errorHandler(error ->
                    log.error("Polling error", error))

                .build()
                .createPublisher();
    }

    /**
     * Example of processing the partitioned stream.
     * Each review is automatically partitioned by its client ID.
     */
    public void startProcessing() {
        createPartitionedPublisher()
                .doOnNext(review ->
                    log.info("Processing review {} for client {}",
                        review.getId(), review.getPartitionKey()))
                .flatMap(this::processReview)
                .subscribe(
                    review -> log.info("Successfully processed review {}", review.getId()),
                    error -> log.error("Error in processing stream", error)
                );
    }

    /**
     * Simulated review processing.
     */
    private reactor.core.publisher.Mono<ReviewQueue> processReview(ReviewQueue review) {
        return reactor.core.publisher.Mono.fromRunnable(() -> {
            // Process the review
            log.info("Processing review type: {}", review.getReviewType());
            // Mark as processed
            review.setProcessed(true);
            reviewQueueRepository.save(review);
        }).thenReturn(review);
    }
}
