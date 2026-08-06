package com.smartcity.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;

public class H2DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseManager.class);
    private static final String DB_URL = "jdbc:h2:./smartcity;AUTO_SERVER=TRUE";
    private static final String USER = "sa";
    private static final String PASS = "";
    
    private static H2DatabaseManager instance;
    
    private H2DatabaseManager() {
        initializeDatabase();
    }
    
    public static synchronized H2DatabaseManager getInstance() {
        if (instance == null) {
            instance = new H2DatabaseManager();
        }
        return instance;
    }
    
    private void initializeDatabase() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create tables for sensor data, alerts, and recommendations
            String createSensorDataTable = 
                "CREATE TABLE IF NOT EXISTS sensor_data (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "sensor_id VARCHAR(50) NOT NULL, " +
                "type VARCHAR(20) NOT NULL, " +
                "value DOUBLE NOT NULL, " +
                "unit VARCHAR(10), " +
                "location VARCHAR(100), " +
                "timestamp TIMESTAMP NOT NULL" +
                ")";
            
            String createAlertsTable = 
                "CREATE TABLE IF NOT EXISTS alerts (" +
                "id VARCHAR(36) PRIMARY KEY, " +
                "sensor_id VARCHAR(50) NOT NULL, " +
                "sensor_type VARCHAR(20) NOT NULL, " +
                "message TEXT, " +
                "severity VARCHAR(10) NOT NULL, " +
                "timestamp TIMESTAMP NOT NULL, " +
                "acknowledged BOOLEAN DEFAULT FALSE" +
                ")";
            
            String createRecommendationsTable = 
                "CREATE TABLE IF NOT EXISTS recommendations (" +
                "id VARCHAR(36) PRIMARY KEY, " +
                "resource VARCHAR(20) NOT NULL, " +
                "action TEXT NOT NULL, " +
                "justification TEXT, " +
                "expected_savings DOUBLE, " +
                "savings_unit VARCHAR(20), " +
                "timestamp TIMESTAMP NOT NULL, " +
                "implemented BOOLEAN DEFAULT FALSE" +
                ")";
            
            stmt.execute(createSensorDataTable);
            stmt.execute(createAlertsTable);
            stmt.execute(createRecommendationsTable);
            
            logger.info("Database tables initialized");
            
        } catch (SQLException e) {
            logger.error("Error initializing database: {}", e.getMessage(), e);
        }
    }
    
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, USER, PASS);
    }
    
    // Sensor Data Methods
    
    public void saveSensorData(SensorData data) {
        String sql = "INSERT INTO sensor_data (sensor_id, type, value, unit, location, timestamp) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, data.getSensorId());
            pstmt.setString(2, data.getType().toString());
            pstmt.setDouble(3, data.getValue());
            pstmt.setString(4, data.getUnit());
            pstmt.setString(5, data.getLocation());
            pstmt.setTimestamp(6, Timestamp.valueOf(data.getTimestamp()));
            
            pstmt.executeUpdate();
            logger.debug("Saved sensor data to H2: {}", data);
            
        } catch (SQLException e) {
            logger.error("Error saving sensor data: {}", e.getMessage(), e);
        }
    }
    
    public List<SensorData> getLatestReadings(String resourceType, int count) {
        List<SensorData> readings = new ArrayList<>();
        String typeCriteria = "";
        
        switch(resourceType.toLowerCase()) {
            case "water":
                typeCriteria = "type IN ('WATER_LEVEL', 'WATER_FLOW')";
                break;
            case "energy":
                typeCriteria = "type = 'ENERGY_CONSUMPTION'";
                break;
            case "waste":
                typeCriteria = "type = 'WASTE_LEVEL'";
                break;
            case "traffic":
                typeCriteria = "type = 'TRAFFIC_DENSITY'";
                break;
            default:
                typeCriteria = "1=1"; // Match all
        }
        
        // Optimized query for real-time monitoring
        // This gets the most recent readings per sensor and type
        String sql = 
            "WITH RankedData AS (" +
            "   SELECT *, " +
            "       ROW_NUMBER() OVER (PARTITION BY sensor_id, type ORDER BY timestamp DESC) as rn " +
            "   FROM sensor_data " +
            "   WHERE " + typeCriteria +
            "   AND timestamp >= DATEADD('MINUTE', -30, CURRENT_TIMESTAMP()) " + // Focus on recent data only
            ") " +
            "SELECT id, sensor_id, type, value, unit, location, timestamp " +
            "FROM RankedData " +
            "WHERE rn = 1 " +
            "ORDER BY timestamp DESC " +
            "LIMIT ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, count);
            long startTime = System.currentTimeMillis();
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    SensorData data = new SensorData();
                    data.setSensorId(rs.getString("sensor_id"));
                    try {
                        data.setType(SensorType.valueOf(rs.getString("type")));
                    } catch (IllegalArgumentException e) {
                        logger.warn("Unknown sensor type: {}", rs.getString("type"));
                        continue; // Skip this record
                    }
                    data.setValue(rs.getDouble("value"));
                    data.setUnit(rs.getString("unit"));
                    data.setLocation(rs.getString("location"));
                    data.setTimestamp(rs.getTimestamp("timestamp").toLocalDateTime());
                    readings.add(data);
                }
            }
            
            long queryTime = System.currentTimeMillis() - startTime;
            logger.debug("Database query for {} readings took {} ms", readings.size(), queryTime);
            
        } catch (SQLException e) {
            logger.error("Error fetching latest readings: {}", e.getMessage(), e);
        }
        
        return readings;
    }
    
    // Alert Methods
    
    public void saveAlert(Alert alert) {
        String sql = "INSERT INTO alerts (id, sensor_id, sensor_type, message, severity, timestamp, acknowledged) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, alert.getId());
            pstmt.setString(2, alert.getSensorId());
            pstmt.setString(3, alert.getSensorType().toString());
            pstmt.setString(4, alert.getMessage());
            pstmt.setString(5, alert.getSeverity().toString());
            pstmt.setTimestamp(6, Timestamp.valueOf(alert.getTimestamp()));
            pstmt.setBoolean(7, alert.isAcknowledged());
            
            pstmt.executeUpdate();
            logger.debug("Saved alert to H2: {}", alert.getMessage());
            
        } catch (SQLException e) {
            logger.error("Error saving alert: {}", e.getMessage(), e);
        }
    }
    
    public List<Alert> getCurrentAlerts(String resourceType) {
        List<Alert> alerts = new ArrayList<>();
        String typeCriteria = "";
        
        switch(resourceType.toLowerCase()) {
            case "water":
                typeCriteria = "sensor_type IN ('WATER_LEVEL', 'WATER_FLOW')";
                break;
            case "energy":
                typeCriteria = "sensor_type = 'ENERGY_CONSUMPTION'";
                break;
            case "waste":
                typeCriteria = "sensor_type = 'WASTE_LEVEL'";
                break;
            case "traffic":
                typeCriteria = "sensor_type = 'TRAFFIC_DENSITY'";
                break;
            default:
                typeCriteria = "1=1"; // Match all
        }
        
        String sql = "SELECT * FROM alerts WHERE " + typeCriteria + 
                     " AND acknowledged = FALSE ORDER BY timestamp DESC";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                Alert alert = new Alert();
                alert.setId(rs.getString("id"));
                alert.setSensorId(rs.getString("sensor_id"));
                alert.setSensorType(SensorType.valueOf(rs.getString("sensor_type")));
                alert.setMessage(rs.getString("message"));
                alert.setSeverity(Alert.Severity.valueOf(rs.getString("severity")));
                alert.setTimestamp(rs.getTimestamp("timestamp").toLocalDateTime());
                alert.setAcknowledged(rs.getBoolean("acknowledged"));
                alerts.add(alert);
            }
            
        } catch (SQLException e) {
            logger.error("Error fetching current alerts: {}", e.getMessage(), e);
        }
        
        return alerts;
    }
    
    public void acknowledgeAlert(String alertId) {
        String sql = "UPDATE alerts SET acknowledged = TRUE WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, alertId);
            int updatedRows = pstmt.executeUpdate();
            
            if (updatedRows > 0) {
                logger.info("Alert acknowledged: {}", alertId);
            }
            
        } catch (SQLException e) {
            logger.error("Error acknowledging alert: {}", e.getMessage(), e);
        }
    }
    
    // Recommendation Methods
    
    public void saveRecommendation(Recommendation rec) {
        String sql = "INSERT INTO recommendations (id, resource, action, justification, expected_savings, " +
                     "savings_unit, timestamp, implemented) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, rec.getId());
            pstmt.setString(2, rec.getResource());
            pstmt.setString(3, rec.getAction());
            pstmt.setString(4, rec.getJustification());
            pstmt.setDouble(5, rec.getExpectedSavings());
            pstmt.setString(6, rec.getSavingsUnit());
            pstmt.setTimestamp(7, Timestamp.valueOf(rec.getTimestamp()));
            pstmt.setBoolean(8, rec.isImplemented());
            
            pstmt.executeUpdate();
            logger.debug("Saved recommendation to H2: {}", rec.getAction());
            
        } catch (SQLException e) {
            logger.error("Error saving recommendation: {}", e.getMessage(), e);
        }
    }
    
    public List<Recommendation> getRecommendations(String resourceType) {
        List<Recommendation> recommendations = new ArrayList<>();
        
        String sql = "SELECT * FROM recommendations WHERE resource = ? " +
                     "AND implemented = FALSE ORDER BY timestamp DESC";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, resourceType);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Recommendation rec = new Recommendation();
                    rec.setId(rs.getString("id"));
                    rec.setResource(rs.getString("resource"));
                    rec.setAction(rs.getString("action"));
                    rec.setJustification(rs.getString("justification"));
                    rec.setExpectedSavings(rs.getDouble("expected_savings"));
                    rec.setSavingsUnit(rs.getString("savings_unit"));
                    rec.setTimestamp(rs.getTimestamp("timestamp").toLocalDateTime());
                    rec.setImplemented(rs.getBoolean("implemented"));
                    recommendations.add(rec);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Error fetching recommendations: {}", e.getMessage(), e);
        }
        
        return recommendations;
    }
    
    public void implementRecommendation(String recommendationId) {
        String sql = "UPDATE recommendations SET implemented = TRUE WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, recommendationId);
            int updatedRows = pstmt.executeUpdate();
            
            if (updatedRows > 0) {
                logger.info("Recommendation implemented: {}", recommendationId);
            }
            
        } catch (SQLException e) {
            logger.error("Error implementing recommendation: {}", e.getMessage(), e);
        }
    }
    
    // Data Generation for Testing
    
    public void generateRandomData() {
        // Generate water level data
        for (int i = 0; i < 10; i++) {
            double level = 20.0 + Math.random() * 60.0; // Between 20-80%
            SensorData data = new SensorData(
                "water-level-" + (i % 3 + 1), 
                SensorType.WATER_LEVEL,
                level,
                "%",
                "Reservoir " + (char)('A' + i % 3)
            );
            data.setTimestamp(LocalDateTime.now().minusMinutes(i * 5));
            saveSensorData(data);
            
            // Generate alert if level is low
            if (level < 30.0) {
                Alert alert = new Alert(
                    data.getSensorId(),
                    SensorType.WATER_LEVEL,
                    "Low water level detected: " + String.format("%.1f", level) + "% at " + data.getLocation(),
                    Alert.Severity.MEDIUM
                );
                alert.setTimestamp(data.getTimestamp());
                saveAlert(alert);
                
                // Generate recommendation
                Recommendation rec = new Recommendation(
                    "Water",
                    "Increase water supply to " + data.getLocation(),
                    "Low water level detected at " + data.getLocation(),
                    30.0 - level,
                    "%"
                );
                rec.setTimestamp(data.getTimestamp());
                saveRecommendation(rec);
            }
        }
        
        // Generate water flow data
        for (int i = 0; i < 10; i++) {
            double flow = 60.0 + Math.random() * 80.0; // Between 60-140 l/min
            SensorData data = new SensorData(
                "water-flow-" + (i % 2 + 1),
                SensorType.WATER_FLOW,
                flow,
                "l/min",
                "Pipeline " + (i % 2 == 0 ? "Main" : "Secondary")
            );
            data.setTimestamp(LocalDateTime.now().minusMinutes(i * 7));
            saveSensorData(data);
            
            // Generate alert if flow is high
            if (flow > 120.0) {
                Alert alert = new Alert(
                    data.getSensorId(),
                    SensorType.WATER_FLOW,
                    "High water flow detected: " + String.format("%.1f", flow) + " l/min at " + data.getLocation(),
                    Alert.Severity.HIGH
                );
                alert.setTimestamp(data.getTimestamp());
                saveAlert(alert);
                
                // Generate recommendation
                Recommendation rec = new Recommendation(
                    "Water",
                    "Reduce water pressure in " + data.getLocation(),
                    "High flow detected, possible leak or overuse",
                    flow - 120.0,
                    "l/min"
                );
                rec.setTimestamp(data.getTimestamp());
                saveRecommendation(rec);
            }
        }
        
        // Generate energy consumption data
        for (int i = 0; i < 10; i++) {
            double consumption = 200.0 + Math.random() * 300.0; // Between 200-500 kW
            SensorData data = new SensorData(
                "energy-" + (i % 3 + 1),
                SensorType.ENERGY_CONSUMPTION,
                consumption,
                "kW",
                "District " + (i % 3 + 1)
            );
            data.setTimestamp(LocalDateTime.now().minusMinutes(i * 10));
            saveSensorData(data);
            
            // Generate alert if consumption is high
            if (consumption > 400.0) {
                Alert alert = new Alert(
                    data.getSensorId(),
                    SensorType.ENERGY_CONSUMPTION,
                    "High energy consumption detected: " + String.format("%.1f", consumption) + " kW in " + data.getLocation(),
                    Alert.Severity.HIGH
                );
                alert.setTimestamp(data.getTimestamp());
                saveAlert(alert);
                
                // Generate recommendation
                Recommendation rec = new Recommendation(
                    "Energy",
                    "Reduce power usage in " + data.getLocation() + " during peak hours",
                    "Excessive consumption detected",
                    consumption - 400.0,
                    "kW"
                );
                rec.setTimestamp(data.getTimestamp());
                saveRecommendation(rec);
            }
        }
        
        // Generate traffic data
        for (int i = 0; i < 10; i++) {
            double density = Math.random() * 90.0; // Between 0-90%
            SensorData data = new SensorData(
                "traffic-" + (i % 4 + 1),
                SensorType.TRAFFIC_DENSITY,
                density,
                "%",
                "Junction " + (char)('A' + i % 4)
            );
            data.setTimestamp(LocalDateTime.now().minusMinutes(i * 8));
            saveSensorData(data);
            
            // Generate alert if density is high
            if (density > 75.0) {
                Alert alert = new Alert(
                    data.getSensorId(),
                    SensorType.TRAFFIC_DENSITY,
                    "Heavy traffic detected: " + String.format("%.1f", density) + "% at " + data.getLocation(),
                    Alert.Severity.MEDIUM
                );
                alert.setTimestamp(data.getTimestamp());
                saveAlert(alert);
                
                // Generate recommendation
                Recommendation rec = new Recommendation(
                    "Traffic",
                    "Adjust traffic lights timing at " + data.getLocation(),
                    "Heavy congestion detected",
                    15.0,
                    "min"
                );
                rec.setTimestamp(data.getTimestamp());
                saveRecommendation(rec);
            }
        }
        
        logger.info("Generated random test data in H2 database");
    }

    public void startWebConsole() {
        try {
            // Start H2 Console for web access
            org.h2.tools.Server webServer = org.h2.tools.Server.createWebServer("-webAllowOthers", "-webPort", "8082")
                    .start();
            logger.info("H2 Web Console started on port 8082");
            logger.info("Access the database at http://localhost:8082/");
        } catch (SQLException e) {
            logger.error("Failed to start H2 Web Console: {}", e.getMessage(), e);
        }
    }
}
