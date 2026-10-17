package com.smartcity.dashboard.rest;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.smartcity.db.DatabaseManager;

@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class HealthResource {
    @GET
    @Path("/live")
    public Response live() {
        return Response.ok(status("live", true)).build();
    }

    @GET
    @Path("/ready")
    public Response ready() {
        boolean databaseReady = DatabaseManager.getInstance().isReady();
        Map<String, Object> payload = status(databaseReady ? "ready" : "not_ready", databaseReady);
        payload.put("database", databaseReady ? "up" : "down");
        return Response.status(databaseReady ? 200 : 503).entity(payload).build();
    }

    private Map<String, Object> status(String state, boolean ready) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", state);
        payload.put("ready", ready);
        return payload;
    }
}
