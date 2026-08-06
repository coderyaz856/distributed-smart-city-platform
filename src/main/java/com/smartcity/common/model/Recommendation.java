package com.smartcity.common.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Recommendation implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String id;
    private String resource;  // water, energy, waste, etc.
    private String action;
    private String justification;
    private double expectedSavings;
    private String savingsUnit;  // kWh, liters, etc.
    private LocalDateTime timestamp;
    private boolean implemented;

    public Recommendation() {
    }

    public Recommendation(String resource, String action, String justification, double expectedSavings, String savingsUnit) {
        this.id = java.util.UUID.randomUUID().toString();
        this.resource = resource;
        this.action = action;
        this.justification = justification;
        this.expectedSavings = expectedSavings;
        this.savingsUnit = savingsUnit;
        this.timestamp = LocalDateTime.now();
        this.implemented = false;
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getJustification() {
        return justification;
    }

    public void setJustification(String justification) {
        this.justification = justification;
    }

    public double getExpectedSavings() {
        return expectedSavings;
    }

    public void setExpectedSavings(double expectedSavings) {
        this.expectedSavings = expectedSavings;
    }

    public String getSavingsUnit() {
        return savingsUnit;
    }

    public void setSavingsUnit(String savingsUnit) {
        this.savingsUnit = savingsUnit;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isImplemented() {
        return implemented;
    }

    public void setImplemented(boolean implemented) {
        this.implemented = implemented;
    }
}
