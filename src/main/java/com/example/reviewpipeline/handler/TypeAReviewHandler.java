package com.example.reviewpipeline.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class TypeAReviewHandler implements ReviewHandler {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getReviewType() {
        return "TYPE_A";
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        return Mono.fromRunnable(() -> {
            try {
                JsonNode json = objectMapper.readTree(reviewMessage);
                log.info("Processing TYPE_A review for client: {}", clientId);
                log.info("Review data: {}", json.toString());

                // Add your custom business logic here
                // For example: call external APIs, update databases, send notifications, etc.

            } catch (Exception e) {
                log.error("Error processing TYPE_A review", e);
                throw new RuntimeException(e);
            }
        });
    }
}
