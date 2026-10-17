package com.smartcity;

import java.io.IOException;
import java.net.URI;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.tyrus.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.communication.rmi.ResourceManagementRMIImpl;
import com.smartcity.communication.socket.SensorSocketServer;
import com.smartcity.config.AppConfig;
import com.smartcity.dashboard.websocket.DashboardWebSocketEndpoint;
import com.smartcity.db.DatabaseManager;
import com.smartcity.kafka.SensorDataConsumer;
import com.smartcity.sensors.SensorSimulator;
import com.smartcity.services.EnergyManagementService;
import com.smartcity.services.ResourceService;
import com.smartcity.services.TrafficManagementService;
import com.smartcity.services.WasteManagementService;
import com.smartcity.services.WaterManagementService;
import com.smartcity.simulation.ContinuousDataGenerator;

public final class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int RMI_PORT = 1099;
    private static final String RMI_NAME = "ResourceManagement";

    private Main() {}

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromEnvironment();
        DatabaseManager database = null;
        try {
            database = DatabaseManager.getInstance();
            if (config.isDemoSeedEnabled()) database.generateRandomData();

            Map<String, ResourceService> services = createServices();
            ContinuousDataGenerator dataGenerator = new ContinuousDataGenerator(services);
            if (config.isContinuousGeneratorEnabled()) dataGenerator.start();

            if (config.isRmiEnabled()) {
                Registry registry = LocateRegistry.createRegistry(RMI_PORT);
                registry.rebind(RMI_NAME, new ResourceManagementRMIImpl());
                logger.info("event=rmi_started port={}", RMI_PORT);
            }

            HttpServer httpServer = startHttpServer(config);
            logger.info("event=http_started host={} port={}", config.getHttpHost(), config.getHttpPort());

            Server webSocketServer = new Server(config.getHttpHost(), config.getWebSocketPort(), "/", null,
                DashboardWebSocketEndpoint.class);
            if (config.isWebSocketEnabled()) {
                webSocketServer.start();
                logger.info("event=websocket_started port={}", config.getWebSocketPort());
            }

            SensorSocketServer socketServer = new SensorSocketServer();
            if (config.isSocketEnabled()) new Thread(socketServer::start, "sensor-socket-server").start();

            SensorDataConsumer kafkaConsumer = null;
            if (config.isKafkaEnabled()) {
                kafkaConsumer = new SensorDataConsumer(services);
                kafkaConsumer.start();
                logger.info("event=kafka_consumer_started bootstrap={}", config.getKafkaBootstrapServers());
            }

            SensorSimulator simulator = config.isSimulatorEnabled()
                ? new SensorSimulator(config.isKafkaEnabled())
                : null;
            if (config.isSimulatorEnabled()) {
                simulator.start();
            }

            CountDownLatch stopped = new CountDownLatch(1);
            final DatabaseManager databaseRef = database;
            final SensorDataConsumer consumerRef = kafkaConsumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (simulator != null) simulator.stop();
                if (config.isSocketEnabled()) socketServer.stop();
                if (config.isContinuousGeneratorEnabled()) dataGenerator.stop();
                if (consumerRef != null) consumerRef.stop();
                httpServer.shutdownNow();
                if (config.isWebSocketEnabled()) webSocketServer.stop();
                databaseRef.close();
                stopped.countDown();
                logger.info("event=application_stopped");
            }, "shutdown"));

            logger.info("event=application_ready");
            stopped.await();
        } catch (Exception error) {
            logger.error("event=startup_failed message={}", error.getMessage(), error);
            if (database != null) database.close();
            System.exit(1);
        }
    }

    private static Map<String, ResourceService> createServices() {
        Map<String, ResourceService> services = new HashMap<>();
        services.put("water", new WaterManagementService());
        services.put("energy", new EnergyManagementService());
        services.put("traffic", new TrafficManagementService());
        services.put("waste", new WasteManagementService());
        return services;
    }

    private static HttpServer startHttpServer(AppConfig config) throws IOException {
        String baseUri = String.format("http://%s:%d/api/", config.getHttpHost(), config.getHttpPort());
        ResourceConfig resources = new ResourceConfig()
            .packages("com.smartcity.dashboard.rest")
            .property(ServerProperties.WADL_FEATURE_DISABLE, true);
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(baseUri), resources, false);
        server.getServerConfiguration().addHttpHandler(new StaticHttpHandler(config.getStaticContentPath()), "/");
        server.start();
        return server;
    }
}
