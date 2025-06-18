#!/bin/bash

# Kafka Reset Script - Clean slate restart
# Usage: ./scripts/reset-kafka.sh

set -e

echo "🔄 Resetting Kafka environment..."

# Stop all services
echo "Stopping Docker services..."
docker-compose down

# Remove volumes (this will delete all data!)
echo "⚠️  Removing all Kafka data volumes..."
read -p "This will delete ALL Kafka data. Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Reset cancelled."
    exit 1
fi

docker-compose down -v
docker system prune -f

# Start services fresh
echo "Starting fresh Kafka services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Verify services
echo "Checking service status..."
docker-compose ps

# Create basic topics
echo "Creating default topics..."
./scripts/create-topics.sh

echo "✅ Kafka environment reset complete!"