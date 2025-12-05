package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.entity.ReviewQueue;
import com.example.reviewpipeline.repository.ReviewQueueRepository;
import com.example.reviewpipeline.service.ClientPartitionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

/**
 * Example service demonstrating how to use JdbcPollingPublisherFactory
 * with Spring-injected configuration from application.yaml.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SpringConfiguredPublisherExample {

    private final JdbcPollingPublisherFactory publisherFactory;
    private final ReviewQueueRepository reviewQueueRepository;
    private final ClientPartitionManager clientPartitionManager;

    /**
     * Creates a publisher using the factory with Spring-injected poll interval.
     * The poll interval comes from application.yaml: publisher.polling.interval-ms
     */
    public Flux<ReviewQueue> createPublisherWithDefaultConfig() {
        JdbcPollingPartitionedPublisher<ReviewQueue> publisher =
                publisherFactory.createPublisher(
                        this::getNextReview,
                        clientPartitionManager::tryAcquireClient,
                        clientPartitionManager::releaseClient
                );

        return publisher.createPublisher();
    }

    /**
     * Creates a publisher with custom configuration,
     * overriding the default poll interval from application.yaml.
     */
    public Flux<ReviewQueue> createPublisherWithCustomInterval() {
        JdbcPollingPartitionedPublisher<ReviewQueue> publisher =
                publisherFactory.createPublisher(
                        this::getNextReview,
                        clientPartitionManager::tryAcquireClient,
                        clientPartitionManager::releaseClient,
                        3000L  // Custom 3-second interval
                );

        return publisher.createPublisher();
    }

    /**
     * Creates a publisher using the builder from factory,
     * allowing full customization while still using defaults from application.yaml.
     */
    public Flux<ReviewQueue> createPublisherWithBuilderCustomization() {
        JdbcPollingPartitionedPublisher<ReviewQueue> publisher =
                publisherFactory.<ReviewQueue>createBuilder()
                        .pollFunction(this::getNextReview)
                        .partitionAcquirer(clientPartitionManager::tryAcquireClient)
                        .partitionReleaser(clientPartitionManager::releaseClient)
                        // These values come from application.yaml by default
                        // but can be overridden here
                        .itemProcessor(review ->
                                log.debug("Processing review: {}", review.getId()))
                        .errorHandler(error ->
                                log.error("Polling error", error))
                        .build();

        return publisher.createPublisher();
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
}
