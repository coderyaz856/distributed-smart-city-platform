package com.smartcity.services;

import java.util.List;
import com.smartcity.services.AIRecommendationService;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;

public interface ResourceService {
    void processSensorData(SensorData data);
    List<SensorData> getLatestReadings(int count);
    List<Alert> getCurrentAlerts();
    List<Recommendation> getRecommendations();
    void acknowledgeAlert(String alertId);
    void implementRecommendation(String recommendationId);
}
