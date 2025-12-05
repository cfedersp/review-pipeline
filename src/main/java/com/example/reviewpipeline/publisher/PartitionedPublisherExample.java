package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import com.example.reviewpipeline.service.ClientPartitionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Example usage of JdbcPollingPublisher with client-based partitioning.
 * This demonstrates how to use the publisher to poll ReviewQueue items
 * while ensuring only one client is processed at a time.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PartitionedPublisherExample {

    private final ReviewQueueRepository reviewQueueRepository;
    private final ClientPartitionManager clientPartitionManager;

    /**
     * Creates a publisher that polls unprocessed reviews with client partitioning.
     * Only one client will be processed at a time, preventing concurrent processing
     * of items from the same client.
     *
     * @return A Flux of ReviewQueue items
     */
    public Flux<ReviewQueue> createPartitionedReviewPublisher() {
        return JdbcPollingPublisher.<ReviewQueue>builder()
                // The JDBC polling function
                .pollFunction(() -> reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(null, false))

                // Extract client ID for partitioning
                .clientIdExtractor(ReviewQueue::getClientFk)

                // Acquire partition lock
                .partitionAcquirer(clientPartitionManager::tryAcquireClient)

                // Release partition lock
                .partitionReleaser(clientPartitionManager::releaseClient)

                // Poll every 5 seconds
                .pollInterval(Duration.ofSeconds(5))

                // Log batch info
                .batchProcessor(batch ->
                    log.info("Polled {} unprocessed reviews", batch.size()))

                // Filter out already processed items (safety check)
                .itemFilter(review -> !review.getProcessed())

                // Handle errors
                .errorHandler(error ->
                    log.error("Error in partitioned polling", error))

                .build()
                .createPublisher();
    }

    /**
     * Creates a publisher for a specific client.
     * This version only polls items for a single client.
     *
     * @param clientFk The client foreign key
     * @return A Flux of ReviewQueue items for the specified client
     */
    public Flux<ReviewQueue> createClientSpecificPublisher(String clientFk) {
        return JdbcPollingPublisher.<ReviewQueue>builder()
                .pollFunction(() ->
                    reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(clientFk, false))
                .pollIntervalSeconds(3)
                .itemFilter(review -> review.getClientFk().equals(clientFk))
                .build()
                .createPublisher();
    }

    /**
     * Creates a hot publisher that shares the stream among multiple subscribers.
     * Useful when multiple processors need to consume the same stream of reviews.
     *
     * @return A hot Flux of ReviewQueue items
     */
    public Flux<ReviewQueue> createSharedPartitionedPublisher() {
        return JdbcPollingPublisher.<ReviewQueue>builder()
                .pollFunction(() -> reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(null, false))
                .clientIdExtractor(ReviewQueue::getClientFk)
                .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                .partitionReleaser(clientPartitionManager::releaseClient)
                .pollIntervalSeconds(5)
                .build()
                .createHotPublisher();
    }
}
