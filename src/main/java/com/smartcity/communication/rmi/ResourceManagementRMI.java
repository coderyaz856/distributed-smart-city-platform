package com.smartcity.communication.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;

public interface ResourceManagementRMI extends Remote {
    void sendSensorData(SensorData data) throws RemoteException;
    List<SensorData> getLatestReadings(String resourceType, int count) throws RemoteException;
    List<Alert> getCurrentAlerts(String resourceType) throws RemoteException;
    List<Recommendation> getRecommendations(String resourceType) throws RemoteException;
    void acknowledgeAlert(String alertId) throws RemoteException;
    void implementRecommendation(String recommendationId) throws RemoteException;
}
