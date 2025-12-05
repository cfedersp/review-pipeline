package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.handler.ReviewHandlerRegistry;
import com.example.reviewpipeline.model.KafkaReviewMessage;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import com.example.reviewpipeline.service.ClientPartitionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * Example service demonstrating how to use ParallelPublisherProcessor
 * with multiple data sources including JdbcPollingPartitionedPublisher.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ParallelPublisherProcessorExample {

    private final ReviewQueueRepository reviewQueueRepository;
    private final ClientPartitionManager clientPartitionManager;
    private final ReviewHandlerRegistry handlerRegistry;

    /**
     * Creates a parallel processor with multiple JDBC polling publishers.
     * Each publisher polls different subsets of data and all are processed in parallel.
     */
    public ParallelPublisherProcessor<ReviewQueue> createMultiSourceProcessor() {
        // Create first publisher for high-priority reviews
        JdbcPollingPartitionedPublisher<ReviewQueue> highPriorityPublisher =
                JdbcPollingPartitionedPublisher.<ReviewQueue>builder()
                        .pollFunction(() -> getNextHighPriorityReview())
                        .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                        .partitionReleaser(clientPartitionManager::releaseClient)
                        .pollIntervalSeconds(2)
                        .build();

        // Create second publisher for normal reviews
        JdbcPollingPartitionedPublisher<ReviewQueue> normalPublisher =
                JdbcPollingPartitionedPublisher.<ReviewQueue>builder()
                        .pollFunction(() -> getNextNormalReview())
                        .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                        .partitionReleaser(clientPartitionManager::releaseClient)
                        .pollIntervalSeconds(5)
                        .build();

        // Combine both publishers with parallel processing
        return ParallelPublisherProcessor.<ReviewQueue>builder()
                .publisher(highPriorityPublisher)
                .publisher(normalPublisher)
                .processor(this::processReviewQueue)
                .maxConcurrency(10)
                .preProcessor(review ->
                        log.info("Processing review {} from client {}",
                                review.getId(), review.getPartitionKey()))
                .successHandler(review ->
                        log.info("Successfully processed review {}", review.getId()))
                .errorHandler((review, error) ->
                        log.error("Failed to process review {}", review.getId(), error))
                .continueOnError(true)
                .build();
    }

    /**
     * Creates a processor that combines Oracle polling and Kafka messages.
     * Demonstrates mixing different types of Partitionable sources.
     */
    public void createMixedSourceProcessor() {
        // JDBC polling publisher
        JdbcPollingPartitionedPublisher<ReviewQueue> jdbcPublisher =
                JdbcPollingPartitionedPublisher.<ReviewQueue>builder()
                        .pollFunction(this::getNextReview)
                        .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                        .partitionReleaser(clientPartitionManager::releaseClient)
                        .pollIntervalSeconds(5)
                        .build();

        // Kafka message stream (simulated as Flux)
        Flux<KafkaReviewMessage> kafkaPublisher = createKafkaMessageFlux();

        // Combine both sources
        ParallelPublisherProcessor<Partitionable> processor =
                ParallelPublisherProcessor.<Partitionable>builder()
                        .publisher(jdbcPublisher.createPublisher())
                        .publisher(kafkaPublisher)
                        .processor(this::processPartitionableItem)
                        .maxConcurrency(15)
                        .build();

        processor.startAsync();
    }

    /**
     * Simple example with single publisher.
     */
    public void startSimpleProcessing() {
        JdbcPollingPartitionedPublisher<ReviewQueue> publisher =
                JdbcPollingPartitionedPublisher.<ReviewQueue>builder()
                        .pollFunction(this::getNextReview)
                        .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                        .partitionReleaser(clientPartitionManager::releaseClient)
                        .pollIntervalSeconds(5)
                        .build();

        ParallelPublisherProcessor.<ReviewQueue>builder()
                .publisher(publisher)
                .processor(this::processReviewQueue)
                .maxConcurrency(10)
                .build()
                .startAsync();
    }

    /**
     * Processes a ReviewQueue item.
     */
    private Mono<Void> processReviewQueue(ReviewQueue review) {
        return handlerRegistry.handleReview(
                review.getReviewType(),
                review.getReviewMessage(),
                review.getClientFk()
        ).then(Mono.fromRunnable(() -> markAsProcessed(review)));
    }

    /**
     * Processes any Partitionable item (polymorphic processing).
     */
    private Mono<Void> processPartitionableItem(Partitionable item) {
        if (item instanceof ReviewQueue) {
            return processReviewQueue((ReviewQueue) item);
        } else if (item instanceof KafkaReviewMessage) {
            return processKafkaMessage((KafkaReviewMessage) item);
        }
        return Mono.empty();
    }

    /**
     * Processes a Kafka review message.
     */
    private Mono<Void> processKafkaMessage(KafkaReviewMessage message) {
        return handlerRegistry.handleReview(
                message.getReviewType(),
                message.getReviewMessage(),
                message.getClientFk()
        );
    }

    /**
     * Retrieves the next unprocessed review from the database.
     */
    private ReviewQueue getNextReview() {
        return reviewQueueRepository.findByClientFkAndProcessedOrderByCreatedDateAsc(null, false)
                .stream()
                .findFirst()
                .orElse(null);
    }

    /**
     * Retrieves the next high-priority review.
     */
    private ReviewQueue getNextHighPriorityReview() {
        // Simulated - would have actual priority logic
        return getNextReview();
    }

    /**
     * Retrieves the next normal priority review.
     */
    private ReviewQueue getNextNormalReview() {
        // Simulated - would have actual priority logic
        return getNextReview();
    }

    /**
     * Marks a review as processed.
     */
    private void markAsProcessed(ReviewQueue review) {
        review.setProcessed(true);
        review.setProcessedDate(LocalDateTime.now());
        reviewQueueRepository.save(review);
        log.info("Marked review {} as processed", review.getId());
    }

    /**
     * Simulated Kafka message stream.
     */
    private Flux<KafkaReviewMessage> createKafkaMessageFlux() {
        // In real implementation, this would connect to Kafka
        return Flux.empty();
    }
}
