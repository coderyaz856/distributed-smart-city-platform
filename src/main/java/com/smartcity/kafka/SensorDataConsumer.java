package com.smartcity.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.services.ResourceService;

public class SensorDataConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SensorDataConsumer.class);
    private final Consumer<String, String> consumer;
    private final Map<String, ResourceService> services = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public SensorDataConsumer(Map<String, ResourceService> services) {
        Properties props = KafkaConfig.getConsumerProperties("smartcity-sensor-consumer");
        this.consumer = new KafkaConsumer<>(props);
        this.services.putAll(services);
        logger.info("Sensor data consumer initialized");
    }

    public void start() {
        consumer.subscribe(Collections.singletonList(KafkaConfig.SENSOR_DATA_TOPIC));
        running.set(true);
        consumerThread = new Thread(this);
        consumerThread.start();
        logger.info("Sensor data consumer started");
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                records.forEach(record -> {
                    logger.debug("Received sensor data: key={}, value={}, partition={}, offset={}",
                            record.key(), record.value(), record.partition(), record.offset());
                    
                    SensorData sensorData = KafkaConfig.deserializeSensorData(record.value());
                    if (sensorData != null) {
                        processData(sensorData);
                    }
                });
                
                // Manual commit
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Error in sensor data consumer: {}", e.getMessage(), e);
        } finally {
            consumer.close();
            logger.info("Sensor data consumer closed");
        }
    }

    private void processData(SensorData data) {
        String resourceType = getResourceTypeFromSensorType(data.getType());
        ResourceService service = services.get(resourceType);
        if (service != null) {
            service.processSensorData(data);
            logger.info("Processed sensor data from Kafka: {}", data);
        } else {
            logger.warn("No service found for resource type: {}", resourceType);
        }
    }

    private String getResourceTypeFromSensorType(SensorType type) {
        switch (type) {
            case WATER_LEVEL:
            case WATER_FLOW:
                return "water";
            case ENERGY_CONSUMPTION:
                return "energy";
            case WASTE_LEVEL:
                return "waste";
            case TRAFFIC_DENSITY:
                return "traffic";
            case AIR_QUALITY:
                return "air";
            default:
                return "other";
        }
    }

    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
        logger.info("Sensor data consumer stopping");
    }
}
