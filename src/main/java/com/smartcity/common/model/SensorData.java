package com.smartcity.common.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class SensorData implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String sensorId;
    private SensorType type;
    private double value;
    private String unit;
    private String location;
    private LocalDateTime timestamp;

    public SensorData() {
    }

    public SensorData(String sensorId, SensorType type, double value, String unit, String location) {
        this.sensorId = sensorId;
        this.type = type;
        this.value = value;
        this.unit = unit;
        this.location = location;
        this.timestamp = LocalDateTime.now();
    }

    // Getters and setters
    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public SensorType getType() {
        return type;
    }

    public void setType(SensorType type) {
        this.type = type;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "sensorId='" + sensorId + '\'' +
                ", type=" + type +
                ", value=" + value +
                ", unit='" + unit + '\'' +
                ", location='" + location + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
