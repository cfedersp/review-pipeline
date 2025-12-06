package com.example.reviewpipeline.model;

import com.example.reviewpipeline.publisher.Partitionable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaReviewMessage implements Partitionable {
    private String clientFk;
    private String accountId;
    private String operation;
    private String reviewType;
    private String reviewMessage;

    @Override
    public String getClientId() {
        return clientFk;
    }
}
