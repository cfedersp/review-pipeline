package com.example.reviewpipeline.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ReviewHandlerRegistry {

    private final Map<String, ReviewHandler> handlers = new ConcurrentHashMap<>();

    public ReviewHandlerRegistry(List<ReviewHandler> reviewHandlers) {
        reviewHandlers.forEach(handler -> {
            handlers.put(handler.getReviewType(), handler);
            log.info("Registered handler for review type: {}", handler.getReviewType());
        });
    }

    public Mono<Void> handleReview(String reviewType, String reviewMessage, String clientId) {
        ReviewHandler handler = handlers.get(reviewType);
        if (handler == null) {
            log.warn("No handler found for review type: {}", reviewType);
            return Mono.error(new IllegalArgumentException("No handler for review type: " + reviewType));
        }
        return handler.handle(reviewMessage, clientId);
    }

    public boolean hasHandler(String reviewType) {
        return handlers.containsKey(reviewType);
    }
}
