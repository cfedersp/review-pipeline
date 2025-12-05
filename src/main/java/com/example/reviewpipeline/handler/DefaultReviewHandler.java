package com.example.reviewpipeline.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class DefaultReviewHandler implements ReviewHandler {

    @Override
    public String getReviewType() {
        return "DEFAULT";
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        return Mono.fromRunnable(() -> {
            log.info("Processing DEFAULT review for client: {}, message: {}", clientId, reviewMessage);
        });
    }
}
