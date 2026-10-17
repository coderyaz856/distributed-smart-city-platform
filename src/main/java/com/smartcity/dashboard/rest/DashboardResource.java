package com.smartcity.dashboard.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartcity.common.model.Alert;
import com.smartcity.common.model.Recommendation;
import com.smartcity.common.model.SensorData;
import com.smartcity.services.EnergyManagementService;
import com.smartcity.services.ResourceService;
import com.smartcity.services.TrafficManagementService;
import com.smartcity.services.WasteManagementService;
import com.smartcity.services.WaterManagementService;

@Path("/dashboard")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DashboardResource {
    
    private final Map<String, ResourceService> services = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(DashboardResource.class);
    private static final Map<String, String> lastDataHashMap = new ConcurrentHashMap<>();
    
    public DashboardResource() {
        // Initialize services
        services.put("water", new WaterManagementService());
        services.put("energy", new EnergyManagementService());
        services.put("traffic", new TrafficManagementService());
        services.put("waste", new WasteManagementService());
    }
    
    @GET
    @Path("/sensors/{resourceType}")
    public Response getLatestReadings(@PathParam("resourceType") String resourceType, 
                                      @QueryParam("count") @DefaultValue("10") int count,
                                      @QueryParam("timestamp") @DefaultValue("0") long timestamp) {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service == null) {
            logger.warn("Resource type not found: {}", resourceType);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("Resource type not found: " + resourceType)
                    .build();
        }
        
        try {
            // Get the latest data
            List<SensorData> readings = service.getLatestReadings(count);
            
            // Create a hash of the data to check if it's changed
            String dataHash = calculateDataHash(readings);
            String lastHash = lastDataHashMap.get(resourceType);
            
            // Store the new hash
            lastDataHashMap.put(resourceType, dataHash);
            
            // Log the data retrieved
            logger.info("Fetched {} readings for resource type: {} (hash: {})", readings.size(), resourceType, dataHash);
            if (!readings.isEmpty()) {
                logger.debug("Latest reading: {}", readings.get(0));
            }
            
            // Set no-cache headers to ensure we always get fresh data
            CacheControl cc = new CacheControl();
            cc.setNoCache(true);
            cc.setNoStore(true);
            cc.setMaxAge(0);
            
            return Response.ok(readings)
                .cacheControl(cc)
                .header("X-Data-Hash", dataHash)
                .header("X-Data-Changed", !dataHash.equals(lastHash))
                .build();
                
        } catch (Exception e) {
            logger.error("Error fetching readings for {}: {}", resourceType, e.getMessage(), e);
            return Response.serverError()
                    .entity("Error fetching readings: " + e.getMessage())
                    .build();
        }
    }
    
    // Helper method to calculate a hash of the data
    private String calculateDataHash(List<SensorData> readings) {
        if (readings.isEmpty()) return "empty";
        
        StringBuilder sb = new StringBuilder();
        for (SensorData data : readings) {
            sb.append(data.getSensorId())
              .append(data.getType())
              .append(data.getValue())
              .append(data.getTimestamp());
        }
        return String.valueOf(sb.toString().hashCode());
    }
    
    @GET
    @Path("/alerts/{resourceType}")
    public Response getCurrentAlerts(@PathParam("resourceType") String resourceType) {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("Resource type not found: " + resourceType)
                    .build();
        }
        
        List<Alert> alerts = service.getCurrentAlerts();
        return Response.ok(alerts).build();
    }
    
    @GET
    @Path("/recommendations/{resourceType}")
    public Response getRecommendations(@PathParam("resourceType") String resourceType) {
        ResourceService service = services.get(resourceType.toLowerCase());
        if (service == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("Resource type not found: " + resourceType)
                    .build();
        }
        
        List<Recommendation> recommendations = service.getRecommendations();
        return Response.ok(recommendations).build();
    }
    
    @POST
    @Path("/alerts/{alertId}/acknowledge")
    public Response acknowledgeAlert(@PathParam("alertId") String alertId) {
        for (ResourceService service : services.values()) {
            service.acknowledgeAlert(alertId);
        }
        return Response.ok().entity("Alert acknowledged: " + alertId).build();
    }
    
    @POST
    @Path("/recommendations/{recommendationId}/implement")
    public Response implementRecommendation(@PathParam("recommendationId") String recommendationId) {
        for (ResourceService service : services.values()) {
            service.implementRecommendation(recommendationId);
        }
        return Response.ok().entity("Recommendation implemented: " + recommendationId).build();
    }
}
