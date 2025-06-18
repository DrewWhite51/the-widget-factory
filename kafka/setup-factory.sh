#!/bin/bash

# Widget Factory Setup Script
# Creates all necessary Kafka topics for the factory simulation

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "🏭 Setting up Widget Factory Kafka topics..."

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

# Widget Factory Topics
echo "Creating component production topic..."
create_topic "component-production" 6 1 604800000  # 7 days

echo "Creating widget assembly topic..."
create_topic "widget-assembly" 3 1 604800000       # 7 days

echo "Creating quality control topic..."
create_topic "quality-control" 3 1 604800000       # 7 days

echo "Creating machine telemetry topic..."
create_topic "machine-telemetry" 12 1 86400000     # 1 day (high volume)

echo "Creating inventory updates topic..."
create_topic "inventory-updates" 6 1 2592000000    # 30 days

echo "Creating production orders topic..."
create_topic "production-orders" 3 1 604800000     # 7 days

echo "Creating alerts topic..."
create_topic "factory-alerts" 1 1 259200000        # 3 days

echo "✅ All Widget Factory topics created successfully!"

# List all topics to verify
echo "📋 Current topics:"
docker exec -it $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "🎯 Widget Factory is ready! You can now run:"
echo "  - Component Production: python component_production_simulator.py"
echo "  - Widget Assembly: python widget_assembly_simulator.py"
echo "  - Quality Control: python quality_control_simulator.py"
echo "  - Machine Telemetry: python machine_telemetry_simulator.py"