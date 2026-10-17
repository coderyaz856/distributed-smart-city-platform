# Distributed Smart City Platform — Academic Prototype

An academic systems prototype for simulated smart-city resource management, built in Java. It explores how sensor readings can move through Kafka into domain services that persist them, evaluate thresholds, raise alerts, and push live updates to a browser dashboard.

The project's focus is **middleware breadth**: five different communication mechanisms — event streaming, remote objects, raw sockets, REST and WebSocket — coexist in one local application, each used where it fits.

> [!IMPORTANT]
> **Status: academic prototype (ENSIAS).** The repository demonstrates the core data path and transport integrations in a local environment. It is not a complete or production-ready smart-city platform.

## Demonstrated scope

- Simulated sensor events published to and consumed from Kafka.
- Water, energy, traffic and waste services behind a shared interface.
- H2 persistence for readings, alerts and recommendations.
- Local access through RMI, raw TCP, REST and WebSocket.
- Threshold-based alerts and an optional external-LLM recommendation path.
- A browser dashboard for observing the simulated stream.

## Prototype boundaries

The repository does not claim multi-node operation, high availability or production-scale validation. It currently has no automated test suite, authentication or authorization, TLS configuration, secrets management, schema migration system, observability stack, load-test evidence, disaster recovery, or deployment manifests for a managed environment. SOAP annotations remain scaffolded, but endpoint publication is disabled because the implementation is incompatible with the targeted Java runtime.

These are explicit boundaries of the prototype rather than completed capabilities.

## Architecture

```
SensorSimulator ──► Kafka topic "sensor-data" ──► SensorDataConsumer
                                                        │
                                                        ▼
                                                 ResourceService
                                        (water │ energy │ traffic │ waste)
                                                        │
                            ┌───────────────────────────┼───────────────────────────┐
                            ▼                           ▼                           ▼
                     H2 persistence            threshold evaluation         WebSocket broadcast
                  sensor_data / alerts /             → Alert                   → live dashboard
                    recommendations                  → Recommendation
```

The same services are additionally reachable over RMI, a raw TCP socket listener, and a REST API, so a reading can enter the system through any of four transports and be handled identically.

### Communication layers

| Mechanism | Implementation | Endpoint |
| --- | --- | --- |
| Event streaming | Kafka producer/consumer, JSON via Jackson | `localhost:9092`, topic `sensor-data` |
| Remote objects | Java RMI registry, `ResourceManagementRMI` | `localhost:1099`, bound as `ResourceManagement` |
| Raw TCP | `ServerSocket` with a 10-thread pool | `localhost:9999` |
| REST | JAX-RS (Jersey) on an embedded Grizzly server | `http://localhost:8080/api/` |
| Push updates | WebSocket (Tyrus) | `ws://localhost:8080/ws` |

The Kafka layer is configured for delivery guarantees rather than defaults: the producer runs idempotent with `acks=all` and retries, and the consumer disables auto-commit in favour of an explicit `commitSync()` after each batch.

SOAP is scaffolded — the services carry JAX-WS `@WebService` / `@WebMethod` annotations — but endpoint publication is commented out in `Main`, since JAX-WS reflection fails on Java 17+.

### Domain services

Four services implement a common `ResourceService` interface and apply their own thresholds:

| Service | Sensor types | Example alert condition |
| --- | --- | --- |
| Water | `WATER_LEVEL`, `WATER_FLOW` | flow > 100 l/min (HIGH), level < 20% (MEDIUM) |
| Energy | `ENERGY_CONSUMPTION` | consumption above configured threshold |
| Traffic | `TRAFFIC_DENSITY` | density above configured threshold |
| Waste | `WASTE_LEVEL` | fill level above configured threshold |

Each reading is persisted, broadcast to connected dashboards, and — when a threshold is crossed — turned into an `Alert`. `AIRecommendationService` is wired in to generate an accompanying `Recommendation` by calling an external LLM API over OkHttp; it reads its endpoint and key from the `GEMINI_ENDPOINT` and `GEMINI_API_KEY` environment variables and stays inactive unless those are set.

### Persistence

Embedded H2 (`jdbc:h2:./smartcity;AUTO_SERVER=TRUE`) with three tables — `sensor_data`, `alerts`, `recommendations` — managed by a singleton `H2DatabaseManager`. The H2 web console starts alongside the application at `http://localhost:8082/`.

## REST API

Base URI `http://localhost:8080/api/`. `{resourceType}` is one of `water`, `energy`, `traffic`, `waste`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/dashboard/sensors/{resourceType}?count=10` | Latest sensor readings |
| `GET` | `/dashboard/alerts/{resourceType}` | Active alerts |
| `GET` | `/dashboard/recommendations/{resourceType}` | Generated recommendations |
| `POST` | `/dashboard/alerts/{alertId}/acknowledge` | Acknowledge an alert |
| `POST` | `/dashboard/recommendations/{recommendationId}/implement` | Mark a recommendation as implemented |

Read endpoints hash their payload and return `304 Not Modified` when the data has not changed, so the dashboard can poll cheaply.

## Requirements

- JDK 11
- Maven 3.6+
- Docker and Docker Compose (for Kafka and ZooKeeper)

## Running

Start Kafka, ZooKeeper and the application together:

```bash
./start-application.sh
```

The script brings up the Compose stack if it is not already running, waits for the broker, then launches the application via `mvn exec:java`. Kafka can also be controlled on its own:

```bash
./manage-kafka.sh start | stop | status
```

Once the log reads `Smart City Resource Management System started successfully`, open `src/main/webapp/index-standalone.html` in a browser and click **Connect** (green button, bottom right) to open the WebSocket stream. Allow a few seconds for the simulator to begin publishing.

To run an already-compiled build without Maven:

```bash
./run-application.sh
```

## Ports

| Port | Service |
| --- | --- |
| 8080 | REST API, static dashboard, WebSocket |
| 8082 | H2 web console |
| 9092 | Kafka broker |
| 9999 | Sensor TCP socket server |
| 2181 | ZooKeeper |
| 1099 | RMI registry |

## Repository structure

```
src/main/java/com/smartcity/
├── Main.java                    Startup: wires and launches every server
├── common/model/                SensorData, Alert, Recommendation, SensorType
├── kafka/                       Producer, consumer, serialization config
├── communication/rmi/           RMI interface and implementation
├── communication/socket/        TCP socket server
├── dashboard/rest/              JAX-RS resources
├── dashboard/websocket/         WebSocket endpoint and broadcasting
├── db/                          H2 schema and data access
├── services/                    Water, energy, traffic and waste services
├── sensors/                     Sensor simulator
└── simulation/                  Continuous data generator

src/main/webapp/                 Browser dashboard
docker-compose.yml               Kafka + ZooKeeper (Confluent 7.3.0)
```

## Technologies

Java 11 · Maven · Apache Kafka · Java RMI · JAX-RS (Jersey) · Grizzly · WebSocket (Tyrus) · H2 · Jackson · OkHttp · Gson · SLF4J/Logback · Docker Compose
