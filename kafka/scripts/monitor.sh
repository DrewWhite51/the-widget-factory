#!/bin/bash

# Kafka Monitoring Script
# Usage: ./scripts/monitor.sh [topic-name]

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC=${1:-"test-topic"}

echo "📊 Kafka Monitoring Dashboard"
echo "=============================="

# Function to get topic info
show_topic_info() {
    echo "📋 Topic Information for: $1"
    docker exec -it $KAFKA_CONTAINER kafka-topics \
        --describe --topic $1 --bootstrap-server $BOOTSTRAP_SERVER
    echo
}

# Function to show consumer groups
show_consumer_groups() {
    echo "👥 Consumer Groups:"
    docker exec -it $KAFKA_CONTAINER kafka-consumer-groups \
        --list --bootstrap-server $BOOTSTRAP_SERVER
    echo
}

# Function to show consumer group details
show_consumer_group_details() {
    local group=$1
    echo "📊 Consumer Group Details: $group"
    docker exec -it $KAFKA_CONTAINER kafka-consumer-groups \
        --describe --group $group --bootstrap-server $BOOTSTRAP_SERVER
    echo
}

# Function to show broker info
show_broker_info() {
    echo "🖥️  Broker Information:"
    docker exec -it $KAFKA_CONTAINER kafka-broker-api-versions \
        --bootstrap-server $BOOTSTRAP_SERVER
    echo
}

# Main monitoring loop
while true; do
    clear
    echo "📊 Kafka Monitoring Dashboard - $(date)"
    echo "=========================================="
    
    # Show all topics
    echo "📋 All Topics:"
    docker exec -it $KAFKA_CONTAINER kafka-topics \
        --list --bootstrap-server $BOOTSTRAP_SERVER
    echo
    
    # Show specific topic details
    if docker exec -it $KAFKA_CONTAINER kafka-topics \
        --list --bootstrap-server $BOOTSTRAP_SERVER | grep -q "^$TOPIC$"; then
        show_topic_info $TOPIC
    else
        echo "⚠️  Topic '$TOPIC' not found"
        echo
    fi
    
    # Show consumer groups
    show_consumer_groups
    
    # Show consumer group details for common groups
    for group in test-group analytics-group user-group; do
        if docker exec -it $KAFKA_CONTAINER kafka-consumer-groups \
            --list --bootstrap-server $BOOTSTRAP_SERVER | grep -q "^$group$"; then
            show_consumer_group_details $group
        fi
    done
    
    echo "Press Ctrl+C to exit, or wait 10 seconds for refresh..."
    sleep 10
done