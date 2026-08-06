package com.smartcity.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.smartcity.common.model.SensorData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    
    // Topic names
    public static final String SENSOR_DATA_TOPIC = "sensor-data";
    
    // Kafka bootstrap servers
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    
    // JSON serializer/deserializer for SensorData
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Enable idempotent producer for exactly once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        logger.info("Kafka producer properties configured");
        return props;
    }
    
    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        logger.info("Kafka consumer properties configured for group: " + groupId);
        return props;
    }
    
    public static String serializeSensorData(SensorData sensorData) {
        try {
            return objectMapper.writeValueAsString(sensorData);
        } catch (Exception e) {
            logger.error("Error serializing sensor data: " + e.getMessage());
            return null;
        }
    }
    
    public static SensorData deserializeSensorData(String json) {
        try {
            return objectMapper.readValue(json, SensorData.class);
        } catch (Exception e) {
            logger.error("Error deserializing sensor data: " + e.getMessage());
            return null;
        }
    }
}
