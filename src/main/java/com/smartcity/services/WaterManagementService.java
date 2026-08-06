// src/main/java/com/smartcity/services/WaterManagementService.java
package com.smartcity.services;

import java.util.List;
import javax.jws.WebMethod;
import javax.jws.WebService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.dashboard.websocket.DashboardWebSocketEndpoint;
import com.smartcity.db.H2DatabaseManager;
import com.smartcity.services.AIRecommendationService;

@WebService(serviceName = "WaterManagementService")
public class WaterManagementService implements ResourceService {
    private static final double WATER_FLOW_HIGH_THRESHOLD = 100.0;  // l/min
    private static final double WATER_LEVEL_LOW_THRESHOLD  = 20.0;   // %

    private final H2DatabaseManager dbManager = H2DatabaseManager.getInstance();
    private final AIRecommendationService aiService = new AIRecommendationService();

    @Override
    @WebMethod
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
            Recommendation rec = aiService.generate(
                data,
                0.0,
                WATER_FLOW_HIGH_THRESHOLD
            );
            dbManager.saveRecommendation(rec);
            DashboardWebSocketEndpoint.broadcastRecommendation(rec);
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

    @Override @WebMethod public List<SensorData>  getLatestReadings(int count)      { return dbManager.getLatestReadings("water", count); }
    @Override @WebMethod public List<Alert>       getCurrentAlerts()                 { return dbManager.getCurrentAlerts("water"); }
    @Override @WebMethod public List<Recommendation> getRecommendations()           { return dbManager.getRecommendations("Water"); }
    @Override @WebMethod public void              acknowledgeAlert(String alertId) { dbManager.acknowledgeAlert(alertId); }
    @Override @WebMethod public void              implementRecommendation(String recommendationId) { dbManager.implementRecommendation(recommendationId); }
}
