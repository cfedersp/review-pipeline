package com.example.reviewpipeline.publisher;

import com.example.reviewpipeline.config.PublisherProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Factory for creating JdbcPollingPartitionedPublisher instances
 * with Spring-injected configuration from application.yaml.
 */
@Component
@RequiredArgsConstructor
public class JdbcPollingPublisherFactory {

    private final PublisherProperties publisherProperties;

    /**
     * Creates a JdbcPollingPartitionedPublisher with default configuration
     * from application.yaml.
     *
     * @param <T> The type of items being polled
     * @param pollFunction The function to poll items from the database
     * @param partitionAcquirer Function to acquire partition locks
     * @param partitionReleaser Consumer to release partition locks
     * @return A configured publisher instance
     */
    public <T extends Partitionable> JdbcPollingPartitionedPublisher<T> createPublisher(
            Callable<T> pollFunction,
            Function<String, Boolean> partitionAcquirer,
            Consumer<String> partitionReleaser) {

        return JdbcPollingPartitionedPublisher.<T>builder()
                .pollFunction(pollFunction)
                .partitionAcquirer(partitionAcquirer)
                .partitionReleaser(partitionReleaser)
                .pollInterval(Duration.ofMillis(publisherProperties.getIntervalMs()))
                .continueOnError(publisherProperties.isContinueOnError())
                .build();
    }

    /**
     * Creates a builder pre-configured with values from application.yaml.
     * Additional settings can be customized via the returned builder.
     *
     * @param <T> The type of items being polled
     * @return A builder with default configuration applied
     */
    public <T extends Partitionable> JdbcPollingPartitionedPublisher.JdbcPollingPartitionedPublisherBuilder<T>
            createBuilder() {
        return JdbcPollingPartitionedPublisher.<T>builder()
                .pollInterval(Duration.ofMillis(publisherProperties.getIntervalMs()))
                .continueOnError(publisherProperties.isContinueOnError());
    }

    /**
     * Creates a publisher with a custom poll interval, overriding the default.
     *
     * @param <T> The type of items being polled
     * @param pollFunction The function to poll items from the database
     * @param partitionAcquirer Function to acquire partition locks
     * @param partitionReleaser Consumer to release partition locks
     * @param pollIntervalMs Custom poll interval in milliseconds
     * @return A configured publisher instance
     */
    public <T extends Partitionable> JdbcPollingPartitionedPublisher<T> createPublisher(
            Callable<T> pollFunction,
            Function<String, Boolean> partitionAcquirer,
            Consumer<String> partitionReleaser,
            long pollIntervalMs) {

        return JdbcPollingPartitionedPublisher.<T>builder()
                .pollFunction(pollFunction)
                .partitionAcquirer(partitionAcquirer)
                .partitionReleaser(partitionReleaser)
                .pollIntervalMs(pollIntervalMs)
                .continueOnError(publisherProperties.isContinueOnError())
                .build();
    }
}
