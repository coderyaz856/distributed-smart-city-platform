package com.smartcity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.SensorType;
import com.smartcity.db.DatabaseManager;
import com.smartcity.services.WaterManagementService;

class WaterPipelineIntegrationTest {
    @BeforeAll
    static void configureDatabase() {
        System.setProperty("DB_URL", "jdbc:h2:mem:water_pipeline;MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        System.setProperty("DB_USER", "sa");
        System.setProperty("DB_PASSWORD", "");
    }

    @Test
    void persistsReadingAndRaisesAlertForLowWaterLevel() {
        WaterManagementService service = new WaterManagementService();
        SensorData reading = new SensorData("water-level-test", SensorType.WATER_LEVEL, 12.5, "%", "test-reservoir");

        service.processSensorData(reading);

        assertTrue(DatabaseManager.getInstance().isReady());
        assertEquals(1, service.getLatestReadings(10).size());
        assertEquals(1, service.getCurrentAlerts().size());
        assertEquals("water-level-test", service.getCurrentAlerts().get(0).getSensorId());
    }
}
