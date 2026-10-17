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

public class EnergyManagementService implements ResourceService {
    
    private static final Logger logger = LoggerFactory.getLogger(EnergyManagementService.class);
    
    // Thresholds
    private static final double ENERGY_HIGH_THRESHOLD = 400.0;  // kW
    private static final double ENERGY_LOW_THRESHOLD  = 0.0;    // (not used but passed to AI)

    private final DatabaseManager dbManager = DatabaseManager.getInstance();
    private final AIRecommendationService aiService = new AIRecommendationService();

    @Override
    public void processSensorData(SensorData data) {
        dbManager.saveSensorData(data);
        DashboardWebSocketEndpoint.broadcastSensorData("energy", data);

        if (data.getType() == SensorType.ENERGY_CONSUMPTION
            && data.getValue() > ENERGY_HIGH_THRESHOLD) {

            // 1) **Alert**
            Alert alert = new Alert(
              data.getSensorId(),
              data.getType(),
              "High energy consumption detected: "
                + data.getValue() + " " + data.getUnit()
                + " at " + data.getLocation(),
              Alert.Severity.HIGH
            );
            dbManager.saveAlert(alert);
            DashboardWebSocketEndpoint.broadcastAlert(alert);

            // 2) **AI-driven recommendation**
            if (aiService.isConfigured()) {
                Recommendation rec = aiService.generate(data, ENERGY_LOW_THRESHOLD, ENERGY_HIGH_THRESHOLD);
                dbManager.saveRecommendation(rec);
                DashboardWebSocketEndpoint.broadcastRecommendation(rec);
            }
        }
    }

    @Override
    public List<SensorData> getLatestReadings(int count) {
        return dbManager.getLatestReadings("energy", count);
    }

    @Override
    public List<Alert> getCurrentAlerts() {
        return dbManager.getCurrentAlerts("energy");
    }

    @Override
    public List<Recommendation> getRecommendations() {
        return dbManager.getRecommendations("Energy");
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
