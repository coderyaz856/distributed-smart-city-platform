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

@WebService(serviceName = "TrafficManagementService")
public class TrafficManagementService implements ResourceService {
    
    private static final Logger logger = LoggerFactory.getLogger(TrafficManagementService.class);
    
    private static final double TRAFFIC_HIGH_THRESHOLD = 80.0;  // %
    private static final double TRAFFIC_LOW_THRESHOLD  = 0.0;

    private final H2DatabaseManager dbManager = H2DatabaseManager.getInstance();
    private final AIRecommendationService aiService = new AIRecommendationService();

    @Override
    @WebMethod
    public void processSensorData(SensorData data) {
        dbManager.saveSensorData(data);
        DashboardWebSocketEndpoint.broadcastSensorData("traffic", data);

        if (data.getType() == SensorType.TRAFFIC_DENSITY
            && data.getValue() > TRAFFIC_HIGH_THRESHOLD) {

            Alert alert = new Alert(
              data.getSensorId(),
              data.getType(),
              "Heavy traffic detected: " + data.getValue() + "% at " + data.getLocation(),
              Alert.Severity.HIGH
            );
            dbManager.saveAlert(alert);
            DashboardWebSocketEndpoint.broadcastAlert(alert);

            Recommendation rec = aiService.generate(
              data,
              TRAFFIC_LOW_THRESHOLD,
              TRAFFIC_HIGH_THRESHOLD
            );
            dbManager.saveRecommendation(rec);
            DashboardWebSocketEndpoint.broadcastRecommendation(rec);
        }
    }

    @Override
    @WebMethod
    public List<SensorData> getLatestReadings(int count) {
        return dbManager.getLatestReadings("traffic", count);
    }

    @Override
    @WebMethod
    public List<Alert> getCurrentAlerts() {
        return dbManager.getCurrentAlerts("traffic");
    }

    @Override
    @WebMethod
    public List<Recommendation> getRecommendations() {
        return dbManager.getRecommendations("Traffic");
    }

    @Override
    @WebMethod
    public void acknowledgeAlert(String alertId) {
        dbManager.acknowledgeAlert(alertId);
    }

    @Override
    @WebMethod
    public void implementRecommendation(String recommendationId) {
        dbManager.implementRecommendation(recommendationId);
    }
}
