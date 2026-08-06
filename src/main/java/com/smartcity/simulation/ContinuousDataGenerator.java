package com.smartcity.simulation;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.dashboard.websocket.DashboardWebSocketEndpoint;
import com.smartcity.services.ResourceService;

/**
 * Generates continuous random sensor data to simulate real-time updates
 */
public class ContinuousDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ContinuousDataGenerator.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Random random = new Random();
    private final Map<String, ResourceService> services;
    
    public ContinuousDataGenerator(Map<String, ResourceService> services) {
        this.services = services;
    }
    
    public void start() {
        // Generate new water level data every 3 seconds
        scheduler.scheduleAtFixedRate(this::generateWaterLevelData, 5, 3, TimeUnit.SECONDS);
        
        // Generate new water flow data every 4 seconds
        scheduler.scheduleAtFixedRate(this::generateWaterFlowData, 6, 4, TimeUnit.SECONDS);
        
        // Generate new energy data every 5 seconds
        scheduler.scheduleAtFixedRate(this::generateEnergyData, 7, 5, TimeUnit.SECONDS);
        
        // Generate new waste data every 6 seconds
        scheduler.scheduleAtFixedRate(this::generateWasteData, 7, 6, TimeUnit.SECONDS);
        
        // Generate new traffic data every 7 seconds
        scheduler.scheduleAtFixedRate(this::generateTrafficData, 8, 7, TimeUnit.SECONDS);
        
        logger.info("Continuous data generation started");
    }
    
    public void stop() {
        scheduler.shutdownNow();
        logger.info("Continuous data generation stopped");
    }
    
    private void generateWaterLevelData() {
        try {
            // Generate random water level between 10% and 90%
            double level = 10.0 + random.nextDouble() * 80.0;
            String sensorId = "water-level-" + (random.nextInt(3) + 1);
            String location = "Reservoir " + (char)('A' + random.nextInt(3));
            
            SensorData data = new SensorData(sensorId, SensorType.WATER_LEVEL, level, "%", location);
            
            // Process data through water service
            ResourceService waterService = services.get("water");
            if (waterService != null) {
                waterService.processSensorData(data);
                logger.debug("Generated water level data: {}", data);
            }
        } catch (Exception e) {
            logger.error("Error generating water level data: {}", e.getMessage(), e);
        }
    }
    
    private void generateWaterFlowData() {
        try {
            // Generate random water flow between 40 and 160 l/min
            double flow = 40.0 + random.nextDouble() * 120.0;
            String sensorId = "water-flow-" + (random.nextInt(2) + 1);
            String location = random.nextBoolean() ? "Main Pipeline" : "Secondary Pipeline";
            
            SensorData data = new SensorData(sensorId, SensorType.WATER_FLOW, flow, "l/min", location);
            
            // Process data through water service
            ResourceService waterService = services.get("water");
            if (waterService != null) {
                waterService.processSensorData(data);
                logger.debug("Generated water flow data: {}", data);
            }
        } catch (Exception e) {
            logger.error("Error generating water flow data: {}", e.getMessage(), e);
        }
    }
    
    private void generateEnergyData() {
        try {
            // Generate random energy consumption between 180 and 520 kW
            double consumption = 180.0 + random.nextDouble() * 340.0;
            String sensorId = "energy-" + (random.nextInt(3) + 1);
            String location = "District " + (random.nextInt(3) + 1);
            
            SensorData data = new SensorData(sensorId, SensorType.ENERGY_CONSUMPTION, consumption, "kW", location);
            
            // Process data through energy service if available
            ResourceService energyService = services.get("energy");
            if (energyService != null) {
                energyService.processSensorData(data);
                logger.debug("Generated energy data: {}", data);
            } else {
                // Fallback to manual broadcast if service not available
                DashboardWebSocketEndpoint.broadcastSensorData("energy", data);
                logger.debug("Generated energy data (direct broadcast): {}", data);
            }
        } catch (Exception e) {
            logger.error("Error generating energy data: {}", e.getMessage(), e);
        }
    }
    
    private void generateTrafficData() {
        try {
            // Generate random traffic density between 0 and 100%
            double density = random.nextDouble() * 100.0;
            String sensorId = "traffic-" + (random.nextInt(4) + 1);
            String location = "Junction " + (char)('A' + random.nextInt(4));
            
            SensorData data = new SensorData(sensorId, SensorType.TRAFFIC_DENSITY, density, "%", location);
            
            // Process data through traffic service if available
            ResourceService trafficService = services.get("traffic");
            if (trafficService != null) {
                trafficService.processSensorData(data);
                logger.debug("Generated traffic data: {}", data);
            } else {
                // Fallback to manual broadcast if service not available
                DashboardWebSocketEndpoint.broadcastSensorData("traffic", data);
                logger.debug("Generated traffic data (direct broadcast): {}", data);
            }
        } catch (Exception e) {
            logger.error("Error generating traffic data: {}", e.getMessage(), e);
        }
    }
    
    // Add waste data generation
    private void generateWasteData() {
        try {
            // Generate random waste level between 10 and 100%
            double level = 10.0 + random.nextDouble() * 90.0;
            String sensorId = "waste-" + (random.nextInt(3) + 1);
            String location = "Container " + (char)('A' + random.nextInt(3));
            
            SensorData data = new SensorData(sensorId, SensorType.WASTE_LEVEL, level, "%", location);
            
            // Process data through waste service if available
            ResourceService wasteService = services.get("waste");
            if (wasteService != null) {
                wasteService.processSensorData(data);
                logger.debug("Generated waste data: {}", data);
            } else {
                // Fallback to manual broadcast if service not available
                DashboardWebSocketEndpoint.broadcastSensorData("waste", data);
                logger.debug("Generated waste data (direct broadcast): {}", data);
            }
        } catch (Exception e) {
            logger.error("Error generating waste data: {}", e.getMessage(), e);
        }
    }
}
