package com.example.reviewpipeline.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaReviewMessage {
    private String reviewType;
    private String clientFk;
    private String reviewMessage;
}
