# Review Pipeline Application

A Spring WebFlux application that implements a reactive processing pipeline for review messages from both Oracle database and Kafka topics.

## Features

- **Dual Source Processing**: Handles review messages from both Oracle database polling and Kafka topics
- **Client-based Partitioning**: Ensures only one client is processed at a time (no concurrent processing per client)
- **Concurrent Job Control**: Configurable maximum concurrent jobs across all clients
- **Dynamic Handler Registration**: Route messages to different handlers based on REVIEW_TYPE
- **Reactive Architecture**: Built with Spring WebFlux and Project Reactor for non-blocking processing

## Architecture

### Components

1. **Oracle Polling Service** (`OraclePollingService`)
   - Polls `REVIEW_QUEUE` table at configurable intervals
   - Groups unprocessed records by `CLIENT_FK`
   - Ensures single-client processing using `ClientPartitionManager`

2. **Kafka Consumer Service** (`KafkaConsumerService`)
   - Listens to configured Kafka topic
   - Deserializes messages into `KafkaReviewMessage` objects
   - Applies same client-based partitioning as Oracle polling

3. **Client Partition Manager** (`ClientPartitionManager`)
   - Maintains lock state for each client
   - Prevents concurrent processing of the same client
   - Uses `ConcurrentHashMap` with `AtomicBoolean` for thread-safe locking

4. **Review Handler Registry** (`ReviewHandlerRegistry`)
   - Auto-discovers all `ReviewHandler` implementations
   - Routes messages to appropriate handler based on `REVIEW_TYPE`
   - Supports extensible handler registration

5. **Review Handlers** (implement `ReviewHandler` interface)
   - `DefaultReviewHandler`: Handles DEFAULT type
   - `TypeAReviewHandler`: Example handler for TYPE_A
   - `TypeBReviewHandler`: Example handler for TYPE_B

## Database Schema

The application expects an Oracle table with the following structure:

```sql
CREATE TABLE REVIEW_QUEUE (
    ID NUMBER PRIMARY KEY,
    CLIENT_FK VARCHAR2(255) NOT NULL,
    REVIEW_TYPE VARCHAR2(50) NOT NULL,
    REVIEW_MESSAGE VARCHAR2(4000) NOT NULL,
    PROCESSED NUMBER(1) DEFAULT 0,
    CREATED_DATE TIMESTAMP,
    PROCESSED_DATE TIMESTAMP
);

CREATE INDEX IDX_REVIEW_QUEUE_CLIENT ON REVIEW_QUEUE(CLIENT_FK, PROCESSED);
CREATE INDEX IDX_REVIEW_QUEUE_PROCESSED ON REVIEW_QUEUE(PROCESSED);
```

## Kafka Message Format

Kafka messages should be JSON with the following structure:

```json
{
  "reviewType": "TYPE_A",
  "clientFk": "CLIENT_123",
  "reviewMessage": "{\"key\":\"value\",\"data\":\"example\"}"
}
```

## Configuration

Update `src/main/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:oracle:thin:@your-host:1521:ORCL
    username: your_username
    password: your_password

  kafka:
    bootstrap-servers: your-kafka-host:9092
    consumer:
      group-id: review-pipeline-group

review:
  pipeline:
    max-concurrent-jobs: 10  # Max concurrent jobs across all clients
    oracle:
      poll-interval-ms: 5000  # Poll every 5 seconds
    kafka:
      topic: review-queue-topic
```

## Adding Custom Handlers

To add a new review type handler:

1. Create a new class implementing `ReviewHandler`:

```java
@Component
@Slf4j
public class MyCustomHandler implements ReviewHandler {

    @Override
    public String getReviewType() {
        return "MY_TYPE";  // Must match REVIEW_TYPE in messages
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        return Mono.fromRunnable(() -> {
            // Your processing logic here
            log.info("Processing MY_TYPE for client: {}", clientId);
        });
    }
}
```

2. The handler will be automatically registered on application startup
3. Messages with `REVIEW_TYPE = "MY_TYPE"` will route to your handler

## How It Works

### Processing Flow

1. **Oracle Polling**:
   - Every 5 seconds (configurable), polls for unprocessed records
   - Groups by `CLIENT_FK` to identify unique clients
   - For each client, attempts to acquire lock via `ClientPartitionManager`
   - If lock acquired, processes all records for that client sequentially
   - Marks records as processed after successful handling
   - Releases client lock when done

2. **Kafka Consumption**:
   - Listens to configured topic
   - On message receipt, attempts to acquire client lock
   - If locked, skips message (will be reprocessed on next poll/retry)
   - If lock acquired, processes message and releases lock

3. **Concurrency Control**:
   - **Client-level**: Only one thread can process a specific client at once
   - **Job-level**: Maximum `max-concurrent-jobs` across all clients (default: 10)
   - Uses `Semaphore` for global job count limiting

### Thread Safety

- Uses `ConcurrentHashMap` for client lock storage
- `AtomicBoolean` for thread-safe lock acquisition
- Reactor's schedulers for non-blocking concurrent processing

## Building and Running

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/review-pipeline-app-1.0.0-SNAPSHOT.jar
```

Or with Maven:
```bash
mvn spring-boot:run
```

## Dependencies

- Spring Boot 3.2.0
- Spring WebFlux
- Spring Data JPA
- Spring Kafka
- Oracle JDBC Driver 23.3.0
- Project Reactor
- Lombok

## Testing

Test inserting records into Oracle:

```sql
INSERT INTO REVIEW_QUEUE (ID, CLIENT_FK, REVIEW_TYPE, REVIEW_MESSAGE, PROCESSED, CREATED_DATE)
VALUES (1, 'CLIENT_001', 'TYPE_A', '{"test": "data"}', 0, SYSTIMESTAMP);

INSERT INTO REVIEW_QUEUE (ID, CLIENT_FK, REVIEW_TYPE, REVIEW_MESSAGE, PROCESSED, CREATED_DATE)
VALUES (2, 'CLIENT_001', 'TYPE_B', '{"test": "data2"}', 0, SYSTIMESTAMP);

INSERT INTO REVIEW_QUEUE (ID, CLIENT_FK, REVIEW_TYPE, REVIEW_MESSAGE, PROCESSED, CREATED_DATE)
VALUES (3, 'CLIENT_002', 'DEFAULT', '{"test": "data3"}', 0, SYSTIMESTAMP);
```

Send Kafka message:
```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic review-queue-topic

{"reviewType":"TYPE_A","clientFk":"CLIENT_001","reviewMessage":"{\"test\":\"kafka data\"}"}
```

## Logging

Set logging levels in `application.yml`:

```yaml
logging:
  level:
    com.example.reviewpipeline: DEBUG  # Application logs
    org.springframework.kafka: INFO    # Kafka logs
    org.hibernate: WARN                # Database logs
```

## License

MIT
