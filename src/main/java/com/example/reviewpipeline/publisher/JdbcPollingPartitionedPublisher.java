package com.example.reviewpipeline.publisher;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A WebFlux publisher that polls a JDBC data source at regular intervals
 * and emits items as a reactive stream with automatic partition-based processing.
 * This publisher requires items to implement the Partitionable interface.
 *
 * @param <T> The type of items being polled, must implement Partitionable
 */
@Slf4j
@Builder
public class JdbcPollingPartitionedPublisher<T extends Partitionable> {

    /**
     * The polling function that retrieves items from the database.
     * This should be a blocking JDBC operation that returns a list of items.
     */
    private final Callable<List<T>> pollFunction;

    /**
     * Function that attempts to acquire a lock for a partition.
     * Should return true if the lock was acquired, false otherwise.
     */
    private final Function<String, Boolean> partitionAcquirer;

    /**
     * Consumer that releases a lock for a partition.
     * Called when processing for a partition is complete.
     */
    private final Consumer<String> partitionReleaser;

    /**
     * The interval between polling attempts.
     * Default: 5 seconds
     */
    @Builder.Default
    private final Duration pollInterval = Duration.ofSeconds(5);

    /**
     * The scheduler to use for blocking JDBC operations.
     * Default: boundedElastic (suitable for blocking I/O)
     */
    @Builder.Default
    private final Scheduler jdbcScheduler = Schedulers.boundedElastic();

    /**
     * Optional function to process each batch of items before emitting.
     * Can be used for logging, metrics, or batch-level transformations.
     */
    private final Consumer<List<T>> batchProcessor;


    /**
     * Whether to continue polling when an error occurs.
     * Default: true (errors are logged and polling continues)
     */
    @Builder.Default
    private final boolean continueOnError = true;

    /**
     * Optional error handler for custom error handling logic.
     */
    private final Consumer<Throwable> errorHandler;

    /**
     * Creates a Flux that polls the database at the specified interval
     * and emits items as they become available.
     *
     * @return A Flux that emits items from the database
     */
    public Flux<T> createPublisher() {
        return Flux.interval(Duration.ZERO, pollInterval)
                .onBackpressureDrop(dropped ->
                    log.warn("Dropped polling tick {} due to backpressure", dropped))
                .flatMap(tick -> pollDatabase())
                .doOnError(error -> {
                    log.error("Error during polling", error);
                    if (errorHandler != null) {
                        errorHandler.accept(error);
                    }
                })
                .retry(continueOnError ? Long.MAX_VALUE : 0);
    }

    /**
     * Creates a Flux that polls the database once and emits all items.
     * Useful for one-time polling operations.
     *
     * @return A Flux that emits items from a single poll
     */
    public Flux<T extends Partitionable> pollOnce() {
        return pollDatabase();
    }

    /**
     * Executes the poll function and returns a Flux of items.
     * If partitioning is enabled, items are grouped by client ID and
     * only processed if the partition lock can be acquired.
     */
    private Flux<T extends Partitionable> pollDatabase() {
        return Mono.fromCallable(pollFunction)
                .subscribeOn(jdbcScheduler)
                .flatMapMany(batch -> {
                    return processWithPartitioning(batch);
                })
                .onErrorResume(error -> {
                    log.error("Error polling database", error);
                    if (errorHandler != null) {
                        errorHandler.accept(error);
                    }
                    return continueOnError ? Flux.empty() : Flux.error(error);
                });
    }

    /**
     * Processes items with partition-based processing.
     * Only emits items for partitions that can acquire a lock.
     * Partition keys are extracted from items using the Partitionable interface.
     */
    private Flux<T extends Partitionable> processWithPartitioning(List<T extends Partitionable> items) {
        return Flux.fromIterable(items)
                .groupBy(Partitionable::getPartitionKey)
                .flatMap(partitionGroup -> {
                    String partitionKey = partitionGroup.key();
                    if (partitionAcquirer != null && partitionAcquirer.apply(partitionKey)) {
                        log.debug("Acquired partition lock for key: {}", partitionKey);
                        return partitionGroup
                                .doFinally(signalType -> {
                                    if (partitionReleaser != null) {
                                        partitionReleaser.accept(partitionKey);
                                        log.debug("Released partition lock for key: {}", partitionKey);
                                    }
                                });
                    } else {
                        log.debug("Skipping partition {} - already in use", partitionKey);
                        return Flux.empty();
                    }
                });
    }

    /**
     * Checks if partitioning is enabled by verifying all required components are present.
     */
    private boolean isPartitioningEnabled() {
        return partitionAcquirer != null;
    }

    /**
     * Creates a hot publisher that starts polling immediately when subscribed
     * and shares the stream among multiple subscribers.
     *
     * @return A hot Flux that shares polling results among subscribers
     */
    public Flux<T> createHotPublisher() {
        return createPublisher()
                .share();
    }

    /**
     * Creates a publisher with controlled concurrency for processing items.
     *
     * @param processor The function to process each item
     * @param concurrency The maximum number of concurrent processing operations
     * @return A Flux that processes items with controlled concurrency
     */
    public Flux<T> createPublisherWithConcurrency(
            Function<T, Mono<T>> processor,
            int concurrency) {
        return createPublisher()
                .flatMap(processor, concurrency);
    }

    /**
     * Builder class for creating JdbcPollingPartitionedPublisher instances with a fluent API.
     */
    public static class JdbcPollingPartitionedPublisherBuilder<T extends Partitionable> {

        /**
         * Sets the poll interval using milliseconds.
         *
         * @param intervalMs The interval in milliseconds
         * @return The builder instance
         */
        public JdbcPollingPartitionedPublisherBuilder<T> pollIntervalMs(long intervalMs) {
            this.pollInterval = Duration.ofMillis(intervalMs);
            return this;
        }

        /**
         * Sets the poll interval using seconds.
         *
         * @param intervalSeconds The interval in seconds
         * @return The builder instance
         */
        public JdbcPollingPartitionedPublisherBuilder<T> pollIntervalSeconds(long intervalSeconds) {
            this.pollInterval = Duration.ofSeconds(intervalSeconds);
            return this;
        }

        /**
         * Configures the publisher to stop on first error.
         *
         * @return The builder instance
         */
        public JdbcPollingPartitionedPublisherBuilder<T> stopOnError() {
            this.continueOnError = false;
            return this;
        }
    }
}
