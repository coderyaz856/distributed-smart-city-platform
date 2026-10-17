package com.smartcity.sensors;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.communication.rmi.ResourceManagementRMI;
import com.smartcity.kafka.SensorDataProducer;

public class SensorSimulator {
    private static final Logger logger = LoggerFactory.getLogger(SensorSimulator.class);
    private static final String SERVER_HOST = "localhost";
    private static final int SOCKET_PORT = 9999;
    private static final int RMI_PORT = 1099;
    private static final String RMI_NAME = "ResourceManagement";
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SensorDataProducer kafkaProducer;
    private final boolean useKafka;
    
    public SensorSimulator(boolean useKafka) {
        this.useKafka = useKafka;
        this.kafkaProducer = useKafka ? new SensorDataProducer() : null;
        logger.info("Kafka integration for sensor simulator: {}", useKafka ? "enabled" : "disabled");
    }
    
    public void start() {
        // Simulate various sensors
        simulateWaterLevelSensor();
        simulateWaterFlowSensor();
        simulateEnergyConsumptionSensor();
        simulateTrafficDensitySensor();
        
        logger.info("Sensor simulator started");
    }
    
    private void simulateWaterLevelSensor() {
        String sensorId = "water-level-001";
        scheduler.scheduleAtFixedRate(() -> {
            double level = 50.0 + random.nextDouble() * 50.0 - 25.0;  // Between 25-75%
            SensorData data = new SensorData(sensorId, SensorType.WATER_LEVEL, level, "%", "Reservoir North");
            
            if (useKafka) {
                sendDataViaKafka(data);
            } else {
                sendDataViaSocket(data);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }
    
    private void simulateWaterFlowSensor() {
        String sensorId = "water-flow-001";
        scheduler.scheduleAtFixedRate(() -> {
            double flow = 50.0 + random.nextDouble() * 100.0;  // Between 50-150 l/min
            SensorData data = new SensorData(sensorId, SensorType.WATER_FLOW, flow, "l/min", "Main Pipeline");
            
            if (useKafka) {
                sendDataViaKafka(data);
            } else {
                sendDataViaRMI(data);
            }
        }, 2, 8, TimeUnit.SECONDS);
    }
    
    private void simulateEnergyConsumptionSensor() {
        String sensorId = "energy-001";
        scheduler.scheduleAtFixedRate(() -> {
            double consumption = 200.0 + random.nextDouble() * 300.0;  // Between 200-500 kW
            SensorData data = new SensorData(sensorId, SensorType.ENERGY_CONSUMPTION, consumption, "kW", "Downtown Grid");
            
            if (useKafka) {
                sendDataViaKafka(data);
            } else {
                sendDataViaSocket(data);
            }
        }, 3, 12, TimeUnit.SECONDS);
    }
    
    private void simulateTrafficDensitySensor() {
        String sensorId = "traffic-001";
        scheduler.scheduleAtFixedRate(() -> {
            double density = random.nextDouble() * 100.0;  // Between 0-100%
            SensorData data = new SensorData(sensorId, SensorType.TRAFFIC_DENSITY, density, "%", "Main Avenue");
            
            if (useKafka) {
                sendDataViaKafka(data);
            } else {
                sendDataViaRMI(data);
            }
        }, 5, 15, TimeUnit.SECONDS);
    }
    
    private void sendDataViaKafka(SensorData data) {
        kafkaProducer.sendSensorData(data);
        logger.info("Sent via Kafka: {}", data);
    }
    
    private void sendDataViaSocket(SensorData data) {
        try (Socket socket = new Socket(SERVER_HOST, SOCKET_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            
            String json = objectMapper.writeValueAsString(data);
            out.println(json);
            logger.info("Sent via socket: {}", data);
            
        } catch (IOException e) {
            logger.error("Error sending data via socket: {}", e.getMessage(), e);
        }
    }
    
    private void sendDataViaRMI(SensorData data) {
        try {
            Registry registry = LocateRegistry.getRegistry(SERVER_HOST, RMI_PORT);
            ResourceManagementRMI rmiService = (ResourceManagementRMI) registry.lookup(RMI_NAME);
            rmiService.sendSensorData(data);
            logger.info("Sent via RMI: {}", data);
        } catch (Exception e) {
            logger.error("Error sending data via RMI: {}", e.getMessage(), e);
        }
    }
    
    public void stop() {
        scheduler.shutdown();
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        logger.info("Sensor simulator stopped");
    }
}
