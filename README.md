# Stock Price Anomaly Detection with Kafka Streams

A real-time stock price processing system built with **Apache Kafka Streams** and **Spring Boot** that demonstrates advanced stream processing patterns including stateful transformations, windowed aggregations, and anomaly detection.

## Overview

This application processes streaming stock price data through two parallel Kafka Streams topologies:

1. **Ticker Topology**: Calculates rolling average prices using hopping time windows
2. **Anomaly Detection Topology**: Identifies price spikes exceeding 5% of historical maximums using stateful processing

## Key Technical Features

### Stream Processing Patterns
- **Stateful Processing**: Custom `FixedKeyProcessor` with persistent state stores for anomaly detection
- **Windowed Aggregations**: 10-second hopping windows with 2-second advancement for real-time averages
- **Modern Processor API**: Used `FixedKeyProcessor` for type-safe, efficient processing
- **Dual Topology Architecture**: Parallel stream branching to avoid key modification side effects

### Spring Boot Integration
- RESTful API for querying Kafka Streams state stores
- Interactive query capabilities with pagination and sorting
- Health check endpoints for monitoring stream readiness
- Kafka consumer integration for real-time alert processing

### Production-Ready Practices
- Persistent key-value stores with proper state management
- Custom serialization/deserialization (JSON, custom Serdes)
- Error handling with global exception handlers
- Structured logging for debugging and monitoring

## Architecture

```
Stock Prices (Kafka Topic)
         |
         v
   StockProcessor
    /           \
   /             \
Ticker         Anomaly
Topology       Topology
   |              |
   v              v
Average      Anomaly Alerts
Prices       (Kafka Topic)
(Kafka Topic)     |
                  v
            AnomalyConsumer
            (Real-time alerts)
```

### Ticker Topology
- Groups stock prices by symbol
- Aggregates within 10-second hopping windows (2-second advance)
- Calculates running averages
- Publishes to `stock-ticker-output` topic

### Anomaly Detection Topology
- Maintains historical maximum price per symbol in state store
- Compares incoming prices against previous max (preventing race conditions)
- Detects spikes > 5% threshold
- Forwards alerts to `stock-anomaly-alerts` topic
- Uses `FixedKeyProcessor` for efficient stateful transformations

## Technology Stack

| Category | Technology |
|----------|-----------|
| **Language** | Java 21 |
| **Framework** | Spring Boot 3.2.0 |
| **Stream Processing** | Apache Kafka Streams |
| **Messaging** | Apache Kafka (Confluent Cloud) |
| **Build Tool** | Maven |
| **Serialization** | Jackson (JSON) |
| **Utilities** | Lombok |

## API Endpoints

### Stock Query API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/stocks/health` | GET | Service health and Kafka Streams readiness |
| `/api/stocks/{symbol}/max-price` | GET | Get maximum historical price for a symbol |
| `/api/stocks/max-prices` | GET | Paginated list of all max prices with sorting |
| `/api/stocks/stats` | GET | State store statistics |

#### Example Requests

```bash
# Check service health
curl http://localhost:8080/api/stocks/health

# Get max price for specific stock
curl http://localhost:8080/api/stocks/AAPL/max-price

# Get paginated max prices, sorted by price descending
curl "http://localhost:8080/api/stocks/max-prices?page=0&size=10&sort=price,desc"

# Get state store statistics
curl http://localhost:8080/api/stocks/stats
```

## Getting Started

### Prerequisites
- Java 21 or higher
- Maven 3.6+
- Access to Kafka cluster (or Confluent Cloud)

### Configuration

Update `src/main/resources/application.yaml` with your Kafka credentials:

```yaml
spring:
  kafka:
    bootstrap-servers: <your-kafka-broker>
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: '<your-credentials>'
```

### Running the Application

```bash
# Build the project
./mvnw clean install

# Run the application
./mvnw spring-boot:run
```

The application will:
1. Connect to Kafka cluster
2. Create required topics if they don't exist
3. Start Kafka Streams processing
4. Expose REST API on port 8080

## Deployment with Docker

### Prerequisites
- Docker and Docker Compose installed
- Confluent Cloud account (or other Kafka cluster)
- Kafka API credentials

### Setup

1. **Create environment file**:
   ```bash
   cp .env.example .env
   ```

2. **Configure Kafka credentials** in `.env`:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=your-kafka-bootstrap-server:9092
   KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_API_KEY" password="YOUR_API_SECRET";
   ```

3. **Build and run with Docker Compose**:
   ```bash
   docker-compose up --build
   ```

   The application will be available at `http://localhost:8080`

4. **Run in detached mode**:
   ```bash
   docker-compose up -d
   ```

5. **View logs**:
   ```bash
   docker-compose logs -f stock-anomaly-detector
   ```

6. **Stop the application**:
   ```bash
   docker-compose down
   ```

### Verifying Deployment

Check the health endpoint to verify the application is running:
```bash
curl http://localhost:8080/api/stocks/health
```

## Future Enhancements

- [ ] Add integration tests with Kafka Streams TopologyTestDriver
- [ ] Implement metrics and monitoring (Prometheus, Grafana)
