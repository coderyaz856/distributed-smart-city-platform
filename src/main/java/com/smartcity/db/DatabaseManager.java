package com.smartcity.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.config.AppConfig;

public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    private final HikariDataSource dataSource;
    private static DatabaseManager instance;
    
    private DatabaseManager() {
        AppConfig config = AppConfig.fromEnvironment();
        HikariConfig pool = new HikariConfig();
        pool.setJdbcUrl(config.getDatabaseUrl());
        pool.setUsername(config.getDatabaseUser());
        pool.setPassword(config.getDatabasePassword());
        pool.setMaximumPoolSize(config.getDatabasePoolSize());
        pool.setMinimumIdle(1);
        pool.setConnectionTimeout(10_000);
        pool.setValidationTimeout(3_000);
        pool.setPoolName("smartcity-db");
        dataSource = new HikariDataSource(pool);
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        logger.info("Database migrations applied");
    }
    
    public static synchronized DatabaseManager getInstance() {
        if (instance == null) {
            instance = new DatabaseManager();
        }
        return instance;
    }
    
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public boolean isReady() {
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
            statement.execute();
            return true;
        } catch (SQLException error) {
            logger.warn("Database readiness check failed: {}", error.getMessage());
            return false;
        }
    }

    public void close() {
        dataSource.close();
        synchronized (DatabaseManager.class) {
            if (instance == this) instance = null;
        }
    }

    private IllegalStateException databaseFailure(String operation, SQLException error) {
        logger.error("Database operation failed: {}", operation, error);
        return new IllegalStateException("Database operation failed: " + operation, error);
    }
    
    // Sensor Data Methods
    
    public void saveSensorData(SensorData data) {
        String sql = "INSERT INTO sensor_data (sensor_id, type, sensor_value, unit, location, timestamp) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, data.getSensorId());
            pstmt.setString(2, data.getType().toString());
            pstmt.setDouble(3, data.getValue());
            pstmt.setString(4, data.getUnit());
            pstmt.setString(5, data.getLocation());
            pstmt.setTimestamp(6, Timestamp.valueOf(data.getTimestamp()));
            
            pstmt.executeUpdate();
            logger.debug("Saved sensor data: {}", data);
            
        } catch (SQLException e) {
            throw databaseFailure("save sensor data", e);
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
            "   AND timestamp >= ? " +
            ") " +
            "SELECT id, sensor_id, type, sensor_value, unit, location, timestamp " +
            "FROM RankedData " +
            "WHERE rn = 1 " +
            "ORDER BY timestamp DESC " +
            "LIMIT ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusMinutes(30)));
            pstmt.setInt(2, count);
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
                    data.setValue(rs.getDouble("sensor_value"));
                    data.setUnit(rs.getString("unit"));
                    data.setLocation(rs.getString("location"));
                    data.setTimestamp(rs.getTimestamp("timestamp").toLocalDateTime());
                    readings.add(data);
                }
            }
            
            long queryTime = System.currentTimeMillis() - startTime;
            logger.debug("Database query for {} readings took {} ms", readings.size(), queryTime);
            
        } catch (SQLException e) {
            throw databaseFailure("fetch latest readings", e);
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
            logger.debug("Saved alert: {}", alert.getMessage());
            
        } catch (SQLException e) {
            throw databaseFailure("save alert", e);
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
            throw databaseFailure("fetch current alerts", e);
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
            throw databaseFailure("acknowledge alert", e);
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
            logger.debug("Saved recommendation: {}", rec.getAction());
            
        } catch (SQLException e) {
            throw databaseFailure("save recommendation", e);
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
            throw databaseFailure("fetch recommendations", e);
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
            throw databaseFailure("implement recommendation", e);
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
        
        logger.info("Generated random demo data");
    }
}
