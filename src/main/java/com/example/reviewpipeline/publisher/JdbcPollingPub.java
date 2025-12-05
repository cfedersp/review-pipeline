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
 * and emits items as a reactive stream.
 *
 * @param <T> The type of items being polled from the database
 */
@Slf4j
@Builder
public class JdbcPollingPublisher<T> {

    /**
     * The polling function that retrieves items from the database.
     * This should be a blocking JDBC operation that returns a list of items.
     */
    private final Callable<List<T>> pollFunction;

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
     * Optional function to filter items before emitting.
     * Return true to emit the item, false to skip it.
     */
    private final Function<T, Boolean> itemFilter;

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
    public Flux<T> pollOnce() {
        return pollDatabase();
    }

    /**
     * Executes the poll function and returns a Flux of items.
     */
    private Flux<T> pollDatabase() {
        return Mono.fromCallable(pollFunction)
                .subscribeOn(jdbcScheduler)
                .doOnNext(batch -> {
                    if (!batch.isEmpty()) {
                        log.debug("Polled {} items from database", batch.size());
                    }
                    if (batchProcessor != null) {
                        batchProcessor.accept(batch);
                    }
                })
                .flatMapMany(Flux::fromIterable)
                .filter(item -> itemFilter == null || itemFilter.apply(item))
                .onErrorResume(error -> {
                    log.error("Error polling database", error);
                    if (errorHandler != null) {
                        errorHandler.accept(error);
                    }
                    return continueOnError ? Flux.empty() : Flux.error(error);
                });
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
     * Builder class for creating JdbcPollingPublisher instances with a fluent API.
     */
    public static class JdbcPollingPublisherBuilder<T> {

        /**
         * Sets the poll interval using milliseconds.
         *
         * @param intervalMs The interval in milliseconds
         * @return The builder instance
         */
        public JdbcPollingPublisherBuilder<T> pollIntervalMs(long intervalMs) {
            this.pollInterval = Duration.ofMillis(intervalMs);
            return this;
        }

        /**
         * Sets the poll interval using seconds.
         *
         * @param intervalSeconds The interval in seconds
         * @return The builder instance
         */
        public JdbcPollingPublisherBuilder<T> pollIntervalSeconds(long intervalSeconds) {
            this.pollInterval = Duration.ofSeconds(intervalSeconds);
            return this;
        }

        /**
         * Configures the publisher to stop on first error.
         *
         * @return The builder instance
         */
        public JdbcPollingPublisherBuilder<T> stopOnError() {
            this.continueOnError = false;
            return this;
        }
    }
}
