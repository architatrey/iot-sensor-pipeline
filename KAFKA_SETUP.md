# Kafka Local Setup Guide

This guide explains how to set up and run a local Kafka cluster for the IoT Sensor Data Producer project using Docker Compose. We are using **KRaft mode**, which means Kafka runs without needing a separate ZooKeeper instance.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed and running.
- [Docker Compose](https://docs.docker.com/compose/install/) installed.

## 1. Create a `docker-compose.yml`

Create a file named `docker-compose.yml` in the root of your project with the following content (already done for you!):

```yaml
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      # KRaft settings
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
      
      # Listener settings
      KAFKA_LISTENERS: 'CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092,PLAINTEXT://kafka:29092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:29092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      
      # Other settings
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

## 2. Start the Kafka Cluster

Run the following command in your terminal from the directory containing `docker-compose.yml`:

```bash
docker-compose up -d
```
*The `-d` flag runs the container in detached mode (in the background).*

To check if the container is running successfully:
```bash
docker-compose ps
```

## 3. Create Required Topics

Based on the project requirements, we need to create the `sensor-readings` topic with 6 partitions. We can also create the stretch-goal `sensor-alerts` topic with 3 partitions.

You can execute Kafka commands inside the running container.

**Create `sensor-readings` topic:**
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic sensor-readings \
  --partitions 6 \
  --replication-factor 1
```

**Create `sensor-alerts` topic:**
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic sensor-alerts \
  --partitions 3 \
  --replication-factor 1
```

**Verify the topics were created:**
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

## 4. Verify Kafka is Working

Before running your Python producer, you can start a console consumer to watch for incoming messages. Open a new terminal window and run:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-readings \
  --from-beginning \
  --property print.key=true \
  --property key.separator="-"
```

When you run your `producer.py` script, you should see the JSON messages streaming into this terminal window.

## 5. Shutting Down

When you are done developing, you can stop the Kafka cluster and remove the containers with:

```bash
docker-compose down
```

*Note: If you want to keep the data between restarts, stop the container using `docker-compose stop` instead of `down`.*
