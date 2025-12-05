package com.example.reviewpipeline.handler;

import reactor.core.publisher.Mono;

public interface ReviewHandler {
    String getReviewType();
    Mono<Void> handle(String reviewMessage, String clientId);
}
