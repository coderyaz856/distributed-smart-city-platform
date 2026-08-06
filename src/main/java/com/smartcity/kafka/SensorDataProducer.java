package com.smartcity.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.common.model.SensorData;

public class SensorDataProducer {
    private static final Logger logger = LoggerFactory.getLogger(SensorDataProducer.class);
    private final Producer<String, String> producer;
    
    public SensorDataProducer() {
        Properties props = KafkaConfig.getProducerProperties();
        this.producer = new KafkaProducer<>(props);
        logger.info("Sensor data producer initialized");
    }
    
    public void sendSensorData(SensorData data) {
        String key = data.getSensorId();
        String value = KafkaConfig.serializeSensorData(data);
        
        if (value != null) {
            try {
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(KafkaConfig.SENSOR_DATA_TOPIC, key, value);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.debug("Sent sensor data to Kafka: {} -> topic={}, partition={}, offset={}",
                                data, metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Error sending sensor data to Kafka: {}", exception.getMessage());
                    }
                }).get(); // Block to ensure delivery for this example
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error sending sensor data to Kafka: {}", e.getMessage());
            }
        }
    }
    
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Sensor data producer closed");
        }
    }
}
