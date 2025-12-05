package com.example.reviewpipeline.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "REVIEW_QUEUE")
@Data
public class ReviewQueue {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;

    @Column(name = "CLIENT_FK", nullable = false)
    private String clientFk;

    @Column(name = "REVIEW_TYPE", nullable = false)
    private String reviewType;

    @Column(name = "REVIEW_MESSAGE", nullable = false, length = 4000)
    private String reviewMessage;

    @Column(name = "PROCESSED")
    private Boolean processed = false;

    @Column(name = "CREATED_DATE")
    private LocalDateTime createdDate;

    @Column(name = "PROCESSED_DATE")
    private LocalDateTime processedDate;

    @PrePersist
    protected void onCreate() {
        createdDate = LocalDateTime.now();
    }
}
