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

public class WasteManagementService implements ResourceService {
    
    private static final Logger logger = LoggerFactory.getLogger(WasteManagementService.class);
    
    private static final double WASTE_HIGH_THRESHOLD = 80.0;  // %
    private static final double WASTE_LOW_THRESHOLD  = 0.0;

    private final DatabaseManager dbManager = DatabaseManager.getInstance();
    private final AIRecommendationService aiService = new AIRecommendationService();

    @Override
    public void processSensorData(SensorData data) {
        dbManager.saveSensorData(data);
        DashboardWebSocketEndpoint.broadcastSensorData("waste", data);

        if (data.getType() == SensorType.WASTE_LEVEL
            && data.getValue() > WASTE_HIGH_THRESHOLD) {

            Alert alert = new Alert(
              data.getSensorId(),
              data.getType(),
              "High waste level detected: " + data.getValue() + "% at " + data.getLocation(),
              Alert.Severity.MEDIUM
            );
            dbManager.saveAlert(alert);
            DashboardWebSocketEndpoint.broadcastAlert(alert);

            if (aiService.isConfigured()) {
                Recommendation rec = aiService.generate(data, WASTE_LOW_THRESHOLD, WASTE_HIGH_THRESHOLD);
                dbManager.saveRecommendation(rec);
                DashboardWebSocketEndpoint.broadcastRecommendation(rec);
            }
        }
    }

    @Override
    public List<SensorData> getLatestReadings(int count) {
        return dbManager.getLatestReadings("waste", count);
    }

    @Override
    public List<Alert> getCurrentAlerts() {
        return dbManager.getCurrentAlerts("waste");
    }

    @Override
    public List<Recommendation> getRecommendations() {
        return dbManager.getRecommendations("Waste");
    }

    @Override
    public void acknowledgeAlert(String alertId) {
        dbManager.acknowledgeAlert(alertId);
    }

    @Override
    public void implementRecommendation(String recommendationId) {
        dbManager.implementRecommendation(recommendationId);
    }
}
