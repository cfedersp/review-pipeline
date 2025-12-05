package com.example.reviewpipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for JDBC polling publishers.
 */
@Component
@ConfigurationProperties(prefix = "publisher.polling")
@Data
public class PublisherProperties {

    /**
     * Default polling interval in milliseconds.
     * Can be overridden in application.yaml with publisher.polling.interval-ms
     */
    private long intervalMs = 5000;

    /**
     * Maximum number of concurrent processing operations.
     */
    private int maxConcurrency = 10;

    /**
     * Whether to continue processing on error.
     */
    private boolean continueOnError = true;
}
