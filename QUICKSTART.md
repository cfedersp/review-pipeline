# Quick Start Guide

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- Docker and Docker Compose (for local testing)

## Option 1: Using Docker Compose (Recommended)

This will start Oracle DB, Kafka, and the application:

```bash
cd ~/projects/review-pipeline-app
docker-compose up -d
```

The application will be available at `http://localhost:8080`

## Option 2: Manual Setup

### 1. Start Oracle Database

Ensure Oracle is running and create the schema:

```bash
sqlplus your_user/your_password@localhost:1521/ORCL
@schema.sql
```

### 2. Start Kafka

```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
kafka-server-start.sh config/server.properties

# Create topic
kafka-topics.sh --create --topic review-queue-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 3. Configure Application

Update `src/main/resources/application.yml` with your Oracle and Kafka credentials.

### 4. Build and Run

```bash
mvn clean package
java -jar target/review-pipeline-app-1.0.0-SNAPSHOT.jar
```

## Testing the Application

### Test Oracle Polling

Connect to Oracle and insert test data:

```sql
INSERT INTO REVIEW_QUEUE (CLIENT_FK, REVIEW_TYPE, REVIEW_MESSAGE, PROCESSED)
VALUES ('CLIENT_001', 'TYPE_A', '{"action": "approve", "amount": 1000}', 0);

COMMIT;
```

Check logs to see the message being processed.

### Test Kafka Consumer

Send a message to Kafka:

```bash
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic review-queue-topic

# Paste this JSON and press Enter:
{"reviewType":"TYPE_A","clientFk":"CLIENT_001","reviewMessage":"{\"action\":\"approve\",\"amount\":2000}"}
```

### Verify Processing

Check application logs:
```bash
tail -f logs/application.log
```

You should see:
```
Processing TYPE_A review for client: CLIENT_001
Marked review ID 1 as processed
```

## Adding Custom Review Handlers

1. Create a new Java class in `src/main/java/com/example/reviewpipeline/handler/`:

```java
@Component
@Slf4j
public class MyCustomHandler implements ReviewHandler {

    @Override
    public String getReviewType() {
        return "MY_CUSTOM_TYPE";
    }

    @Override
    public Mono<Void> handle(String reviewMessage, String clientId) {
        return Mono.fromRunnable(() -> {
            log.info("Processing custom review for client: {}", clientId);
            // Your business logic here
        });
    }
}
```

2. Rebuild and restart the application
3. Send messages with `reviewType: "MY_CUSTOM_TYPE"`

## Configuration Options

Edit `application.yml`:

```yaml
review:
  pipeline:
    max-concurrent-jobs: 10      # Max parallel jobs
    oracle:
      poll-interval-ms: 5000     # How often to poll Oracle (ms)
    kafka:
      topic: review-queue-topic  # Kafka topic name
```

## Monitoring

Check unprocessed records:
```sql
SELECT CLIENT_FK, REVIEW_TYPE, PROCESSED, CREATED_DATE
FROM REVIEW_QUEUE
WHERE PROCESSED = 0
ORDER BY CREATED_DATE;
```

Check processing statistics:
```sql
SELECT REVIEW_TYPE,
       COUNT(*) as TOTAL,
       SUM(CASE WHEN PROCESSED = 1 THEN 1 ELSE 0 END) as PROCESSED_COUNT
FROM REVIEW_QUEUE
GROUP BY REVIEW_TYPE;
```

## Troubleshooting

### Application won't start

- Check Oracle is running: `lsnrctl status`
- Check Kafka is running: `kafka-topics.sh --list --bootstrap-server localhost:9092`
- Verify credentials in `application.yml`

### Messages not being processed

- Check logs for errors
- Verify REVIEW_TYPE has a registered handler
- Ensure CLIENT_FK is not already being processed (wait for release)

### Database connection errors

- Verify Oracle connection string
- Check firewall rules
- Ensure Oracle user has necessary permissions

## Next Steps

- Implement custom handlers for your review types
- Add monitoring and metrics
- Configure production database connection pools
- Set up distributed tracing
- Add integration tests

## Support

For issues, check the logs in:
- Application: `logs/application.log`
- Docker containers: `docker-compose logs -f`
