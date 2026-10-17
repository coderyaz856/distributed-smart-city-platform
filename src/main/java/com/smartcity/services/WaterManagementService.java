// src/main/java/com/smartcity/services/WaterManagementService.java
package com.smartcity.services;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.dashboard.websocket.DashboardWebSocketEndpoint;
import com.smartcity.db.DatabaseManager;
import com.smartcity.services.AIRecommendationService;

public class WaterManagementService implements ResourceService {
    private static final double WATER_FLOW_HIGH_THRESHOLD = 100.0;  // l/min
    private static final double WATER_LEVEL_LOW_THRESHOLD  = 20.0;   // %

    private final DatabaseManager dbManager = DatabaseManager.getInstance();
    private final AIRecommendationService aiService = new AIRecommendationService();

    @Override
    public void processSensorData(SensorData data) {
        dbManager.saveSensorData(data);
        DashboardWebSocketEndpoint.broadcastSensorData("water", data);

        if (data.getType() == SensorType.WATER_FLOW
                && data.getValue() > WATER_FLOW_HIGH_THRESHOLD) {

            Alert alert = new Alert(
                data.getSensorId(),
                data.getType(),
                "High water flow detected: " 
                    + data.getValue() + " " + data.getUnit() 
                    + " at " + data.getLocation(),
                Alert.Severity.HIGH
            );
            dbManager.saveAlert(alert);
            DashboardWebSocketEndpoint.broadcastAlert(alert);

            // Génération IA
            if (aiService.isConfigured()) {
                Recommendation rec = aiService.generate(data, 0.0, WATER_FLOW_HIGH_THRESHOLD);
                dbManager.saveRecommendation(rec);
                DashboardWebSocketEndpoint.broadcastRecommendation(rec);
            }
        }

        if (data.getType() == SensorType.WATER_LEVEL
                && data.getValue() < WATER_LEVEL_LOW_THRESHOLD) {

            Alert alert = new Alert(
                data.getSensorId(),
                data.getType(),
                "Low water level detected: " 
                    + data.getValue() + "% at " + data.getLocation(),
                Alert.Severity.MEDIUM
            );
            dbManager.saveAlert(alert);
            DashboardWebSocketEndpoint.broadcastAlert(alert);
        }
    }

    @Override public List<SensorData>  getLatestReadings(int count)      { return dbManager.getLatestReadings("water", count); }
    @Override public List<Alert>       getCurrentAlerts()                 { return dbManager.getCurrentAlerts("water"); }
    @Override public List<Recommendation> getRecommendations()           { return dbManager.getRecommendations("Water"); }
    @Override public void              acknowledgeAlert(String alertId) { dbManager.acknowledgeAlert(alertId); }
    @Override public void              implementRecommendation(String recommendationId) { dbManager.implementRecommendation(recommendationId); }
}
