#!/bin/bash

case "$1" in
    start)
        echo "Starting Kafka and ZooKeeper..."
        docker-compose up -d
        echo "Waiting for Kafka to be ready..."
        sleep 10
        ;;
    stop)
        echo "Shutting down Kafka and ZooKeeper..."
        docker-compose down
        ;;
    status)
        if [ "$(docker ps -q -f name=kafka)" != "" ]; then
            echo "Kafka container is running"
        else
            echo "Kafka container is not running"
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac
