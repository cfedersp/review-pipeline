package com.example.reviewpipeline.model;

import com.example.reviewpipeline.publisher.Partitionable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaReviewMessage implements Partitionable {
    private String reviewType;
    private String clientFk;
    private String reviewMessage;

    @Override
    public String getPartitionKey() {
        return clientFk;
    }
}
