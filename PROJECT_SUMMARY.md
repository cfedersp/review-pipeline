# Project Summary: Review Pipeline Application

## Overview

A production-ready Spring WebFlux application implementing a reactive processing pipeline that handles review messages from both Oracle database and Kafka topics, with client-based partitioning and concurrent job control.

## Key Features Implemented

### 1. Dual Source Processing
- **Oracle Database Polling**: Polls `REVIEW_QUEUE` table every 5 seconds (configurable)
- **Kafka Consumer**: Real-time consumption from Kafka topic
- Both sources feed into the same processing pipeline

### 2. Client-Based Partitioning
- Uses `ClientPartitionManager` with ConcurrentHashMap + AtomicBoolean
- Ensures only ONE client is processed at a time (no concurrent processing per client)
- Thread-safe lock acquisition and release
- Applies to both Oracle and Kafka sources

### 3. Concurrent Job Control
- Global `Semaphore` limits max concurrent jobs (default: 10)
- Jobs can run in parallel across different clients
- Prevents system overload while maximizing throughput

### 4. Dynamic Handler Routing
- Handler registry auto-discovers all `ReviewHandler` implementations
- Routes messages to appropriate handler based on `REVIEW_TYPE` field
- Extensible: just add a new `@Component` implementing `ReviewHandler`

### 5. Reactive Architecture
- Built with Spring WebFlux and Project Reactor
- Non-blocking I/O for high throughput
- Backpressure-aware processing

## Project Structure

```
review-pipeline-app/
├── src/main/java/com/example/reviewpipeline/
│   ├── ReviewPipelineApplication.java          # Main application class
│   ├── config/
│   │   └── KafkaConsumerConfig.java            # Kafka consumer configuration
│   ├── entity/
│   │   └── ReviewQueue.java                    # JPA entity for Oracle table
│   ├── model/
│   │   ├── ReviewMessage.java                  # Base review message
│   │   └── KafkaReviewMessage.java             # Kafka-specific message
│   ├── repository/
│   │   └── ReviewQueueRepository.java          # JPA repository
│   ├── service/
│   │   ├── OraclePollingService.java           # Oracle polling logic
│   │   ├── KafkaConsumerService.java           # Kafka consumption logic
│   │   └── ClientPartitionManager.java         # Client lock manager
│   └── handler/
│       ├── ReviewHandler.java                  # Handler interface
│       ├── ReviewHandlerRegistry.java          # Handler registry
│       ├── DefaultReviewHandler.java           # Default handler
│       ├── TypeAReviewHandler.java             # Example TYPE_A handler
│       └── TypeBReviewHandler.java             # Example TYPE_B handler
├── src/main/resources/
│   └── application.yml                         # Application configuration
├── src/test/java/
│   └── .../ClientPartitionManagerTest.java     # Unit tests
├── schema.sql                                  # Oracle database schema
├── docker-compose.yml                          # Docker setup
├── Dockerfile                                  # Application container
├── pom.xml                                     # Maven dependencies
├── README.md                                   # Full documentation
├── QUICKSTART.md                               # Quick start guide
└── .gitignore                                  # Git ignore rules
```

## Database Schema

The `REVIEW_QUEUE` table includes:
- `ID`: Primary key (auto-generated)
- `CLIENT_FK`: Client identifier (for partitioning)
- `REVIEW_TYPE`: Type of review (routes to specific handler)
- `REVIEW_MESSAGE`: JSON payload containing instructions
- `PROCESSED`: Boolean flag (0=pending, 1=processed)
- `CREATED_DATE`: Timestamp of creation
- `PROCESSED_DATE`: Timestamp when processed

## Kafka Message Structure

```json
{
  "reviewType": "TYPE_A",
  "clientFk": "CLIENT_123",
  "reviewMessage": "{\"key\":\"value\"}"
}
```

## How Concurrency Works

### Client-Level Partitioning
```
CLIENT_001: [Processing] ← Lock acquired
CLIENT_002: [Processing] ← Different client, can run concurrently
CLIENT_003: [Waiting]    ← Lock available, will be picked up
CLIENT_001: [Waiting]    ← Same client, must wait for lock release
```

### Job-Level Control
```
Max Jobs: 10
Currently Running: 8
New Job: ✓ Accepted (9/10 slots used)
New Job: ✓ Accepted (10/10 slots used)
New Job: ✗ Waiting (all slots full, will retry)
```

## Configuration Points

Located in `application.yml`:

```yaml
review.pipeline.max-concurrent-jobs       # Global job limit
review.pipeline.oracle.poll-interval-ms   # Oracle poll frequency
review.pipeline.kafka.topic               # Kafka topic name
spring.datasource.*                       # Oracle connection
spring.kafka.*                            # Kafka connection
```

## Adding New Review Types

1. Create a new handler class:
```java
@Component
@Slf4j
public class MyTypeHandler implements ReviewHandler {
    @Override
    public String getReviewType() {
        return "MY_TYPE";
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        // Your logic here
    }
}
```

2. Restart application (handler auto-registered)
3. Send messages with `reviewType: "MY_TYPE"`

## Running the Application

### Quick Start (Docker Compose)
```bash
cd ~/projects/review-pipeline-app
docker-compose up -d
```

### Manual Build
```bash
mvn clean package
java -jar target/review-pipeline-app-1.0.0-SNAPSHOT.jar
```

## Testing

### Oracle Test
```sql
INSERT INTO REVIEW_QUEUE (CLIENT_FK, REVIEW_TYPE, REVIEW_MESSAGE, PROCESSED)
VALUES ('CLIENT_001', 'TYPE_A', '{"test": "data"}', 0);
```

### Kafka Test
```bash
echo '{"reviewType":"TYPE_A","clientFk":"CLIENT_001","reviewMessage":"{}"}' | \
  kafka-console-producer --bootstrap-server localhost:9092 --topic review-queue-topic
```

## Dependencies

- Spring Boot 3.2.0
- Spring WebFlux (reactive web framework)
- Spring Data JPA (Oracle integration)
- Spring Kafka (Kafka integration)
- Oracle JDBC Driver 23.3.0
- Project Reactor (reactive streams)
- Lombok (code generation)

## Design Decisions

### Why WebFlux?
- Non-blocking I/O for high throughput
- Efficient resource utilization
- Natural fit for event-driven processing

### Why Semaphore + AtomicBoolean?
- Semaphore: Simple, efficient global job limiting
- AtomicBoolean: Lock-free client partitioning
- No need for distributed locks (single-instance deployment)

### Why Separate Models?
- `ReviewQueue` (JPA Entity): Oracle-specific with JPA annotations
- `KafkaReviewMessage`: Kafka-specific, JSON-serializable
- Clean separation of concerns, easier to maintain

### Why Handler Registry?
- Extensibility: Add handlers without modifying core code
- Type safety: Compile-time checking of review types
- Automatic discovery: Spring auto-wires all handlers

## Production Considerations

Before deploying to production:

1. **Database Connection Pooling**: Configure HikariCP settings
2. **Kafka Consumer Groups**: Adjust for horizontal scaling
3. **Monitoring**: Add Micrometer metrics and health checks
4. **Error Handling**: Implement dead letter queues
5. **Distributed Locks**: Consider Redis for multi-instance deployments
6. **Logging**: Configure centralized logging (ELK, Splunk)
7. **Security**: Enable SSL/TLS for Kafka and Oracle
8. **Resource Limits**: Tune thread pools and memory settings

## Next Steps

1. Implement your specific business logic in custom handlers
2. Add integration tests for Oracle and Kafka flows
3. Set up CI/CD pipeline
4. Configure monitoring and alerting
5. Add API endpoints for manual triggering (if needed)
6. Implement audit logging for compliance

## License

MIT License - Free to use and modify
