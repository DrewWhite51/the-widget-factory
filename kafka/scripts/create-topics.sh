#!/bin/bash

# Kafka Topic Creation Script
# Usage: ./scripts/create-topics.sh

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "🚀 Creating Kafka topics..."

# Function to create topic
create_topic() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication=${3:-1}
    local retention_ms=${4:-604800000}  # 7 days default
    
    echo "Creating topic: $topic_name (partitions: $partitions, replication: $replication)"
    
    docker exec -it $KAFKA_CONTAINER kafka-topics --create \
        --topic $topic_name \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --partitions $partitions \
        --replication-factor $replication \
        --config retention.ms=$retention_ms \
        --if-not-exists
}

# Create standard topics
create_topic "test-topic" 3 1
create_topic "user-events" 6 1 86400000    # 1 day retention
create_topic "system-logs" 12 1 2592000000 # 30 days retention
create_topic "analytics" 3 1 604800000     # 7 days retention
create_topic "notifications" 1 1 172800000 # 2 days retention

# Create topics for examples
create_topic "batch-processing" 6 1
create_topic "error-handling" 3 1
create_topic "dlq-topic" 1 1  # Dead Letter Queue
create_topic "order-events" 3 1
create_topic "user-events" 3 1

echo "✅ All topics created successfully!"

# List all topics to verify
echo "📋 Current topics:"
docker exec -it $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER