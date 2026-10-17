package com.smartcity.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import com.smartcity.common.model.SensorData;
import com.smartcity.common.model.Recommendation;

import java.io.IOException;

public class AIRecommendationService {
    private static final String ENDPOINT = System.getenv("GEMINI_ENDPOINT");
    private final String apiKey = System.getenv("GEMINI_API_KEY");
    private final OkHttpClient client = new OkHttpClient();
    private final Gson gson = new Gson();

    public boolean isConfigured() {
        return ENDPOINT != null && !ENDPOINT.trim().isEmpty()
            && apiKey != null && !apiKey.trim().isEmpty();
    }

    public Recommendation generate(SensorData data, double lowThreshold, double highThreshold) {
        if (!isConfigured()) {
            throw new IllegalStateException("AI recommendation service is not configured");
        }
        String prompt = String.format(
          "Contexte: capteur %s (%s @ %s). Valeur=%.2f%s. Seuils [bas=%.2f, haut=%.2f]. Proposez une recommandation actionnable.",
          data.getSensorId(), data.getType(), data.getLocation(),
          data.getValue(), data.getUnit(), lowThreshold, highThreshold);

        JsonObject payload = new JsonObject();
        payload.addProperty("prompt", prompt);
        payload.addProperty("max_tokens", 150);

        RequestBody body = RequestBody.create(
          gson.toJson(payload),
          MediaType.get("application/json; charset=utf-8")
        );
        Request request = new Request.Builder()
          .url(ENDPOINT)
          .addHeader("Authorization", "Bearer " + apiKey)
          .post(body)
          .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP " + response.code());
            }
            JsonObject root = gson.fromJson(response.body().string(), JsonObject.class);
            String text = root.get("recommendation").getAsString();
            // ← **Use the 5-arg constructor**: resource, action, justification, expectedSavings, savingsUnit
            return new Recommendation(
              data.getType().name(),
              text,
              "IA-Gemini",
              0.0,
              data.getUnit()
            );
        } catch (IOException e) {
            throw new RuntimeException("Erreur appel Gemini", e);
        }
    }
}
