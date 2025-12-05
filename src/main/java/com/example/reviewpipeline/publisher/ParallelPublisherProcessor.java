package com.example.reviewpipeline.publisher;

import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A processor that combines multiple publishers (including JdbcPollingPartitionedPublisher)
 * and processes items from all sources in parallel.
 *
 * @param <T> The type of items being processed, must implement Partitionable
 */
@Slf4j
@Builder
public class ParallelPublisherProcessor<T extends Partitionable> {

    /**
     * List of publishers to merge and process.
     * Each publisher can be a JdbcPollingPartitionedPublisher or any other Flux<T>.
     */
    @Singular
    private final List<Flux<T>> publishers;

    /**
     * The processing function to apply to each item.
     * Should return a Mono that completes when processing is done.
     */
    private final Function<T, Mono<Void>> processor;

    /**
     * Maximum number of items to process concurrently.
     * Default: 10
     */
    @Builder.Default
    private final int maxConcurrency = 10;

    /**
     * The scheduler to use for processing operations.
     * Default: parallel scheduler
     */
    @Builder.Default
    private final Scheduler processingScheduler = Schedulers.parallel();

    /**
     * Optional handler called when an item is successfully processed.
     */
    private final Consumer<T> successHandler;

    /**
     * Optional handler called when processing an item fails.
     */
    private final BiConsumer<T, Throwable> errorHandler;

    /**
     * Whether to continue processing when an error occurs.
     * Default: true (errors are logged and processing continues)
     */
    @Builder.Default
    private final boolean continueOnError = true;

    /**
     * Optional consumer called before processing each item.
     * Can be used for logging or metrics.
     */
    private final Consumer<T> preProcessor;

    /**
     * Starts processing items from all publishers in parallel.
     * Returns a Flux that emits items as they are processed.
     *
     * @return A Flux that emits processed items
     */
    public Flux<T> start() {
        if (publishers == null || publishers.isEmpty()) {
            log.warn("No publishers configured");
            return Flux.empty();
        }

        log.info("Starting parallel processing with {} publishers and max concurrency {}",
                publishers.size(), maxConcurrency);

        return Flux.merge(publishers)
                .doOnNext(item -> {
                    if (preProcessor != null) {
                        preProcessor.accept(item);
                    }
                    log.debug("Received item from partition: {}", item.getPartitionKey());
                })
                .flatMap(item -> processItem(item)
                        .thenReturn(item)
                        .onErrorResume(error -> handleError(item, error)),
                    maxConcurrency)
                .subscribeOn(processingScheduler);
    }

    /**
     * Processes a single item using the configured processor.
     */
    private Mono<Void> processItem(T item) {
        return processor.apply(item)
                .doOnSuccess(v -> {
                    log.debug("Successfully processed item from partition: {}",
                            item.getPartitionKey());
                    if (successHandler != null) {
                        successHandler.accept(item);
                    }
                })
                .doOnError(error ->
                    log.error("Error processing item from partition: {}",
                            item.getPartitionKey(), error));
    }

    /**
     * Handles errors during item processing.
     */
    private Mono<T> handleError(T item, Throwable error) {
        log.error("Failed to process item from partition: {}",
                item.getPartitionKey(), error);

        if (errorHandler != null) {
            errorHandler.accept(item, error);
        }

        return continueOnError ? Mono.empty() : Mono.error(error);
    }

    /**
     * Starts processing and subscribes to the stream.
     * This is a fire-and-forget method that begins processing in the background.
     */
    public void startAsync() {
        start().subscribe(
                item -> log.debug("Item processed: {}", item),
                error -> log.error("Fatal error in processing pipeline", error),
                () -> log.info("Processing pipeline completed")
        );
    }

    /**
     * Functional interface for error handling with item and throwable.
     */
    @FunctionalInterface
    public interface BiConsumer<T, U> {
        void accept(T t, U u);
    }

    /**
     * Builder class with additional helper methods.
     */
    public static class ParallelPublisherProcessorBuilder<T extends Partitionable> {

        /**
         * Adds a JdbcPollingPartitionedPublisher as a data source.
         *
         * @param publisher The partitioned publisher
         * @return The builder instance
         */
        public ParallelPublisherProcessorBuilder<T> publisher(
                JdbcPollingPartitionedPublisher<T> publisher) {
            return this.publisher(publisher.createPublisher());
        }

        /**
         * Sets a simple error handler that only receives the throwable.
         *
         * @param simpleErrorHandler Consumer that handles errors
         * @return The builder instance
         */
        public ParallelPublisherProcessorBuilder<T> simpleErrorHandler(
                Consumer<Throwable> simpleErrorHandler) {
            this.errorHandler = (item, error) -> simpleErrorHandler.accept(error);
            return this;
        }

        /**
         * Configures the processor to stop on first error.
         *
         * @return The builder instance
         */
        public ParallelPublisherProcessorBuilder<T> stopOnError() {
            this.continueOnError = false;
            return this;
        }
    }
}
