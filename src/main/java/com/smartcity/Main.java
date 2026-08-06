package com.smartcity;

import java.io.IOException;
import java.net.URI;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.xml.ws.Endpoint;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.glassfish.tyrus.server.Server;  // Tyrus WebSocket server
import com.smartcity.dashboard.websocket.DashboardWebSocketEndpoint;

import com.smartcity.communication.rmi.ResourceManagementRMIImpl;
import com.smartcity.communication.socket.SensorSocketServer;
import com.smartcity.db.H2DatabaseManager;
import com.smartcity.kafka.SensorDataConsumer;
import com.smartcity.sensors.SensorSimulator;
import com.smartcity.services.EnergyManagementService;
import com.smartcity.services.ResourceService;
import com.smartcity.services.TrafficManagementService;
import com.smartcity.services.WasteManagementService;
import com.smartcity.services.WaterManagementService;
import com.smartcity.simulation.ContinuousDataGenerator;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int RMI_PORT = 1099;
    private static final String RMI_NAME = "ResourceManagement";
    private static final String BASE_URI = "http://localhost:8080/api/";
    private static final String STATIC_CONTENT_PATH = "./src/main/webapp";
    private static final boolean USE_KAFKA = true; // Set to true to use Kafka
    
    public static void main(String[] args) {
        try {
            // Initialize and populate database
            H2DatabaseManager dbManager = H2DatabaseManager.getInstance();
            dbManager.generateRandomData();
            
            // Start H2 Web Console
            dbManager.startWebConsole();
            logger.info("H2 Web Console available at http://localhost:8082/");
            logger.info("Use JDBC URL: jdbc:h2:./smartcity (embedded) or jdbc:h2:tcp://localhost/./smartcity (server)");
            
            // Resource services map
            Map<String, ResourceService> services = new HashMap<>();
            services.put("water", new WaterManagementService());
            services.put("energy", new EnergyManagementService());
            services.put("traffic", new TrafficManagementService());
            services.put("waste", new WasteManagementService());
            
            // Start continuous data generator
            ContinuousDataGenerator dataGenerator = new ContinuousDataGenerator(services);
            dataGenerator.start();
            logger.info("Continuous data generator started");
            
            // Start RMI service
            ResourceManagementRMIImpl rmiService = new ResourceManagementRMIImpl();
            Registry registry = LocateRegistry.createRegistry(RMI_PORT);
            registry.rebind(RMI_NAME, rmiService);
            logger.info("RMI service started on port " + RMI_PORT);
            
            // Disable SOAP (reflection errors on Java 17+)
            /*
            String soapEndpoint = "http://localhost:8081/ws/waterservice";
            Endpoint.publish(soapEndpoint, new WaterManagementService());
            logger.info("SOAP service published at " + soapEndpoint);
            */
            
            // Start REST API
            HttpServer httpServer = startHttpServer();
            logger.info("REST API started at " + BASE_URI);
            logger.info("Static content served from " + STATIC_CONTENT_PATH);
            logger.info("Dashboard available at http://localhost:8080/");
            
            // Start WebSocket server for dashboard
            Server wsServer = new Server("localhost", 8080, "/", null, DashboardWebSocketEndpoint.class);
            try {
                wsServer.start();
                logger.info("WebSocket endpoint published at ws://localhost:8080/ws/sensors");
            } catch (Exception e) {
                logger.error("Unable to start WebSocket: {}", e.getMessage(), e);
            }
            
            // Start sensor socket server
            SensorSocketServer socketServer = new SensorSocketServer();
            new Thread(socketServer::start).start();
            
            // Kafka consumer
            final KafkaConsumerHolder kafkaHolder = new KafkaConsumerHolder();
            if (USE_KAFKA) {
                kafkaHolder.consumer = new SensorDataConsumer(services);
                kafkaHolder.consumer.start();
                logger.info("Kafka sensor data consumer started");
            }
            
            // Sensor simulator
            SensorSimulator simulator = new SensorSimulator();
            if (USE_KAFKA) simulator.enableKafka(true);
            simulator.start();
            
            // Keep running
            CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                simulator.stop();
                socketServer.stop();
                dataGenerator.stop();
                if (kafkaHolder.consumer != null) kafkaHolder.consumer.stop();
                httpServer.shutdownNow();
                wsServer.stop();
                latch.countDown();
                logger.info("Application shutting down");
            }));
            
            logger.info("Smart City Resource Management System started successfully");
            logger.info("Press Ctrl+C to stop the application");
            latch.await();
        } catch (Exception e) {
            logger.error("Error starting application: {}", e.getMessage(), e);
        }
    }
    
    private static HttpServer startHttpServer() throws IOException {
        ResourceConfig config = new ResourceConfig().packages("com.smartcity.dashboard.rest");
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), config);
        server.getServerConfiguration().addHttpHandler(new StaticHttpHandler(STATIC_CONTENT_PATH), "/");
        server.start();
        return server;
    }
    
    // Holder for Kafka consumer
    private static class KafkaConsumerHolder {
        SensorDataConsumer consumer;
    }
}
