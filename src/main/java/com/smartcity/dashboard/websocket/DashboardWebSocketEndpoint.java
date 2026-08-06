// src/main/java/com/smartcity/dashboard/websocket/DashboardWebSocketEndpoint.java
package com.smartcity.dashboard.websocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;

@ServerEndpoint("/ws")
public class DashboardWebSocketEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(DashboardWebSocketEndpoint.class);
    private static final Set<Session> sessions = new CopyOnWriteArraySet<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    static {
        // Ping every 30s to keep connections alive
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ping();
            } catch (Exception e) {
                logger.error("Error in ping scheduler", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    @OnOpen
    public void onOpen(Session session) {
        sessions.add(session);
        logger.info("New WebSocket connection: " + session.getId());
    }
    
    @OnClose
    public void onClose(Session session) {
        sessions.remove(session);
        logger.info("WebSocket connection closed: " + session.getId());
    }
    
    @OnError
    public void onError(Session session, Throwable error) {
        logger.error("WebSocket error: " + error.getMessage(), error);
        sessions.remove(session);
    }
    
    @OnMessage
    public void onMessage(String message, Session session) {
        // Pas d'usage pour les messages entrants pour l'instant
    }
    
    public static void broadcastSensorData(String resourceType, SensorData data) {
        send("sensorData", Map.of(
            "resourceType", resourceType,
            "payload", data
        ));
        logger.debug("Broadcast sensorData {} -> {}", resourceType, data);
    }
    
    public static void broadcastAlert(Alert alert) {
        send("alert", Map.of("payload", alert));
    }
    
    public static void broadcastRecommendation(Recommendation rec) {
        send("recommendation", Map.of("payload", rec));
    }
    
    private static void send(String type, Map<String,Object> body) {
        try {
            var message = new HashMap<String,Object>();
            message.put("type", type);
            message.putAll(body);
            String json = objectMapper.writeValueAsString(message);
            
            int success=0, fail=0;
            for (Session s : sessions) {
                if (s.isOpen()) {
                    try {
                        s.getBasicRemote().sendText(json);
                        success++;
                    } catch (IOException ex) {
                        fail++;
                        logger.error("WS send error", ex);
                    }
                }
            }
            if (success>0 || fail>0) {
                logger.debug("WS broadcast {}: success={}, fail={}", type, success, fail);
            }
        } catch (Exception e) {
            logger.error("Error building WS message", e);
        }
    }
    
    private static void ping() {
        send("ping", Map.of("timestamp", System.currentTimeMillis()));
        logger.debug("Ping to {} sessions", sessions.size());
    }
}
