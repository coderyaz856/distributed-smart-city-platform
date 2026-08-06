#!/bin/bash

# Check if Kafka is already running
if [ "$(docker ps -q -f name=kafka)" = "" ]; then
    # Start Kafka and ZooKeeper using Docker Compose
    echo "Starting Kafka and ZooKeeper..."
    docker-compose up -d

    # Wait for Kafka to be ready
    echo "Waiting for Kafka to be ready..."
    sleep 10
else
    echo "Kafka is already running, reusing existing containers"
fi

# Start the application using the Maven exec plugin to include all dependencies
echo "Starting Smart City Resource Management System..."
mvn exec:java -Dexec.mainClass="com.smartcity.Main"

# Ask user if they want to shut down containers
read -p "Do you want to shut down Kafka and ZooKeeper containers? (y/N): " answer
if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
    echo "Shutting down Kafka and ZooKeeper..."
    docker-compose down
else
    echo "Leaving Kafka and ZooKeeper running"
fi
