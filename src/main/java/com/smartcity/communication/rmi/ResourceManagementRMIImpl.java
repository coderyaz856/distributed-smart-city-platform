package com.smartcity.communication.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.services.ResourceService;
import com.smartcity.services.WaterManagementService;
import com.smartcity.services.EnergyManagementService;
import com.smartcity.services.TrafficManagementService;
import com.smartcity.services.WasteManagementService;

public class ResourceManagementRMIImpl extends UnicastRemoteObject implements ResourceManagementRMI {
    
    private final Map<String, ResourceService> services;
    
    public ResourceManagementRMIImpl() throws RemoteException {
        super();
        services = new HashMap<>();
        // Initialize services
        services.put("water", new WaterManagementService());
        services.put("energy", new EnergyManagementService());
        services.put("traffic", new TrafficManagementService());
        services.put("waste", new WasteManagementService());
    }
    
    @Override
    public void sendSensorData(SensorData data) throws RemoteException {
        String resourceType = getResourceTypeFromSensorType(data.getType());
        ResourceService service = services.get(resourceType);
        if (service != null) {
            service.processSensorData(data);
        }
    }

    @Override
    public List<SensorData> getLatestReadings(String resourceType, int count) throws RemoteException {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service != null) {
            return service.getLatestReadings(count);
        }
        return List.of();
    }

    @Override
    public List<Alert> getCurrentAlerts(String resourceType) throws RemoteException {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service != null) {
            return service.getCurrentAlerts();
        }
        return List.of();
    }

    @Override
    public List<Recommendation> getRecommendations(String resourceType) throws RemoteException {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service != null) {
            return service.getRecommendations();
        }
        return List.of();
    }

    @Override
    public void acknowledgeAlert(String alertId) throws RemoteException {
        for (ResourceService service : services.values()) {
            service.acknowledgeAlert(alertId);
        }
    }

    @Override
    public void implementRecommendation(String recommendationId) throws RemoteException {
        for (ResourceService service : services.values()) {
            service.implementRecommendation(recommendationId);
        }
    }
    
    private String getResourceTypeFromSensorType(SensorType type) {
        switch (type) {
            case WATER_LEVEL:
            case WATER_FLOW:
                return "water";
            case ENERGY_CONSUMPTION:
                return "energy";
            case WASTE_LEVEL:
                return "waste";
            case TRAFFIC_DENSITY:
                return "traffic";
            case AIR_QUALITY:
                return "air";
            default:
                return "other";
        }
    }
}
