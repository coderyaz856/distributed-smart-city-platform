package com.smartcity.config;

/** Centralized, environment-driven runtime configuration. */
public final class AppConfig {
    private final String databaseUrl;
    private final String databaseUser;
    private final String databasePassword;
    private final int databasePoolSize;
    private final String kafkaBootstrapServers;
    private final String httpHost;
    private final int httpPort;
    private final int webSocketPort;

    private AppConfig() {
        databaseUrl = value("DB_URL", "jdbc:h2:./smartcity;MODE=PostgreSQL;AUTO_SERVER=TRUE");
        databaseUser = value("DB_USER", "sa");
        databasePassword = value("DB_PASSWORD", "");
        databasePoolSize = integer("DB_MAX_POOL_SIZE", 10);
        kafkaBootstrapServers = value("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        httpHost = value("HTTP_HOST", "0.0.0.0");
        httpPort = integer("PORT", 8080);
        webSocketPort = integer("WEBSOCKET_PORT", 8081);
    }

    public static AppConfig fromEnvironment() { return new AppConfig(); }

    private static String value(String name, String fallback) {
        String property = System.getProperty(name);
        if (property != null && !property.trim().isEmpty()) return property;
        String environment = System.getenv(name);
        return environment == null || environment.trim().isEmpty() ? fallback : environment;
    }

    private static boolean bool(String name, boolean fallback) {
        return Boolean.parseBoolean(value(name, Boolean.toString(fallback)));
    }

    private static int integer(String name, int fallback) {
        try { return Integer.parseInt(value(name, Integer.toString(fallback))); }
        catch (NumberFormatException error) { throw new IllegalArgumentException(name + " must be an integer", error); }
    }

    public String getDatabaseUrl() { return databaseUrl; }
    public String getDatabaseUser() { return databaseUser; }
    public String getDatabasePassword() { return databasePassword; }
    public int getDatabasePoolSize() { return databasePoolSize; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getHttpHost() { return httpHost; }
    public int getHttpPort() { return httpPort; }
    public int getWebSocketPort() { return webSocketPort; }
    public String getStaticContentPath() { return value("STATIC_CONTENT_PATH", "./src/main/webapp"); }
    public boolean isKafkaEnabled() { return bool("KAFKA_ENABLED", true); }
    public boolean isSimulatorEnabled() { return bool("SIMULATOR_ENABLED", false); }
    public boolean isContinuousGeneratorEnabled() { return bool("CONTINUOUS_GENERATOR_ENABLED", false); }
    public boolean isRmiEnabled() { return bool("RMI_ENABLED", false); }
    public boolean isSocketEnabled() { return bool("SOCKET_ENABLED", false); }
    public boolean isWebSocketEnabled() { return bool("WEBSOCKET_ENABLED", true); }
    public boolean isDemoSeedEnabled() { return bool("DEMO_SEED_DATA", false); }
}
