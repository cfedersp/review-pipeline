package com.example.reviewpipeline.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "reviewType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ReviewMessage.class, name = "DEFAULT")
})
public class ReviewMessage {
    private String reviewType;
    private String clientId;
    private String payload;
}
