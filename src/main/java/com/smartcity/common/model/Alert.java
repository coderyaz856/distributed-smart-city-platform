package com.smartcity.common.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Alert implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public enum Severity {
        LOW, MEDIUM, HIGH, CRITICAL
    }
    
    private String id;
    private String sensorId;
    private SensorType sensorType;
    private String message;
    private Severity severity;
    private LocalDateTime timestamp;
    private boolean acknowledged;

    public Alert() {
    }

    public Alert(String sensorId, SensorType sensorType, String message, Severity severity) {
        this.id = java.util.UUID.randomUUID().toString();
        this.sensorId = sensorId;
        this.sensorType = sensorType;
        this.message = message;
        this.severity = severity;
        this.timestamp = LocalDateTime.now();
        this.acknowledged = false;
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public SensorType getSensorType() {
        return sensorType;
    }

    public void setSensorType(SensorType sensorType) {
        this.sensorType = sensorType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }
}
