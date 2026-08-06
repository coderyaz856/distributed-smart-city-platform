package com.smartcity.communication.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.services.ResourceService;
import com.smartcity.services.WaterManagementService;
import com.smartcity.services.EnergyManagementService;
import com.smartcity.services.TrafficManagementService;
import com.smartcity.services.WasteManagementService;

public class SensorSocketServer {
    private static final Logger logger = LoggerFactory.getLogger(SensorSocketServer.class);
    private static final int PORT = 9999;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, ResourceService> services = new HashMap<>();
    private boolean running = false;
    
    public SensorSocketServer() {
        // Initialize services
        services.put("water", new WaterManagementService());
        services.put("energy", new EnergyManagementService());
        services.put("traffic", new TrafficManagementService());
        services.put("waste", new WasteManagementService());
        // Add other services as needed
    }
    
    public void start() {
        running = true;
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            logger.info("Socket server started on port " + PORT);
            
            while (running) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            logger.error("Error in socket server: " + e.getMessage(), e);
        } finally {
            stop();
        }
    }
    
    public void stop() {
        running = false;
        executorService.shutdown();
        logger.info("Socket server stopped");
    }
    
    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            String line;
            StringBuilder jsonBuilder = new StringBuilder();
            
            while ((line = in.readLine()) != null) {
                jsonBuilder.append(line);
            }
            
            String json = jsonBuilder.toString();
            SensorData sensorData = objectMapper.readValue(json, SensorData.class);
            
            // Process sensor data
            String resourceType = getResourceTypeFromSensorType(sensorData.getType());
            ResourceService service = services.get(resourceType);
            if (service != null) {
                service.processSensorData(sensorData);
                logger.info("Processed sensor data: " + sensorData);
            } else {
                logger.warn("No service found for resource type: " + resourceType);
            }
            
        } catch (IOException e) {
            logger.error("Error handling client connection: " + e.getMessage(), e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error("Error closing client socket: " + e.getMessage(), e);
            }
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
}
