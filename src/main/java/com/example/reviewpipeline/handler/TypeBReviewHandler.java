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
public class TypeBReviewHandler implements ReviewHandler {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getReviewType() {
        return "TYPE_B";
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        return Mono.fromRunnable(() -> {
            try {
                JsonNode json = objectMapper.readTree(reviewMessage);
                log.info("Processing TYPE_B review for client: {}", clientId);
                log.info("Review data: {}", json.toString());

                // Add your custom business logic here
                // Different processing logic than TYPE_A

            } catch (Exception e) {
                log.error("Error processing TYPE_B review", e);
                throw new RuntimeException(e);
            }
        });
    }
}
