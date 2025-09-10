# Watermark Messaging Consumer Setup

This project includes a `WatermarkMessagingConsumer` that demonstrates Kafka consumption using `ConsumerRecord` with custom metadata.

## Features

- **Kafka Consumer**: Uses `@Incoming` channel to consume from `watermark-topic`
- **ConsumerRecord Processing**: Processes `ConsumerRecord<String, String>` instead of `Message`
- **Kafka Headers**: Uses actual Kafka headers for sender and timestamp metadata
- **Startup Messages**: Automatically sends test messages on application startup

## Running the Application

### 1. Start Kafka Infrastructure

```bash
# Start Kafka and Kafka UI using Docker Compose
docker-compose up -d

# Verify Kafka is running
docker-compose ps
```

### 2. Start the Quarkus Application

```bash
# Development mode
./mvnw quarkus:dev

# Or using Maven wrapper
mvn quarkus:dev
```

### 3. Access Kafka UI

Open your browser and go to: http://localhost:8080

## Message Format

The consumer processes messages with Kafka headers:
- **Message Content**: Plain text message
- **Kafka Headers**: 
  - `sender`: sender-1 or sender-2
  - `sender-timestamp`: Epoch timestamp in milliseconds

Example:
```
Message: "Watermark message 1"
Headers: 
  - sender: "sender-1"
  - sender-timestamp: "1703123456789"
```

## Configuration

The application is configured in `application.properties`:

- **Topic**: `watermark-topic`
- **Bootstrap Servers**: `localhost:9092`
- **Auto Offset Reset**: `earliest`
- **Serializers**: String for both key and value

## Testing

1. Start the application
2. Check the console output for consumed messages
3. Use Kafka UI to inspect the `watermark-topic`
4. Verify that messages include sender and timestamp metadata

## Stopping

```bash
# Stop the application (Ctrl+C)
# Stop Kafka infrastructure
docker-compose down
```
