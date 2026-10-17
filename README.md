# Distributed Smart City Platform — Academic Prototype

An academic systems prototype for simulated smart-city resource management, built in Java. It explores how sensor readings can move through Kafka into domain services that persist them, evaluate thresholds, raise alerts, and push live updates to a browser dashboard.

The project's focus is **middleware breadth**: five different communication mechanisms — event streaming, remote objects, raw sockets, REST and WebSocket — coexist in one local application, each used where it fits.

> [!IMPORTANT]
> **Status: academic prototype (ENSIAS).** The repository demonstrates the core data path and transport integrations in a local environment. It is not a complete or production-ready smart-city platform.

## Demonstrated scope

- Simulated sensor events published to and consumed from Kafka.
- Water, energy, traffic and waste services behind a shared interface.
- PostgreSQL persistence in the container profile, with H2 retained for local tests.
- Local access through RMI, raw TCP, REST and WebSocket.
- Threshold-based alerts and an optional external-LLM recommendation path.
- A browser dashboard for observing the simulated stream.

## Prototype boundaries

The repository does not claim multi-node operation, high availability or production-scale validation. Authentication and authorization, TLS termination, managed secret storage, load-test evidence, disaster recovery, and a provider-specific deployment are still outstanding.

These are explicit boundaries of the prototype rather than completed capabilities.

## Production foundation

The repository now includes a deployable baseline around one tested vertical slice:

- a multi-stage, non-root Java 17 container with a readiness health check;
- environment-driven database, Kafka, ports and optional transport configuration;
- pooled PostgreSQL connections through HikariCP and versioned Flyway migrations;
- liveness and database-backed readiness endpoints;
- JSON application logs suitable for collection by a managed runtime;
- graceful Kafka consumer wake-up and process shutdown;
- a Docker Compose stack with PostgreSQL 16 and Kafka in KRaft mode;
- an integration test that sends a low-water reading through the water service and verifies both persistence and alert creation.

RMI, raw TCP, simulators, demo seeding and the continuous generator are disabled by default. They can be enabled explicitly for local demonstrations. The AI recommendation path is also inert unless both its endpoint and API key are configured.

The original breadth remains available. `SensorSimulator` can drive Kafka or the enabled local RMI and raw-TCP paths; `ContinuousDataGenerator` exercises the domain services directly; and the seed-data mode provides a deterministic way to populate the dashboard without external infrastructure.

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
                     PostgreSQL / H2            threshold evaluation         WebSocket broadcast
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
| Push updates | WebSocket (Tyrus) | `ws://localhost:8081/ws` |

The Kafka layer is configured for delivery guarantees rather than defaults: the producer runs idempotent with `acks=all` and retries, and the consumer disables auto-commit in favour of an explicit `commitSync()` after each batch.

The production baseline removes the inactive SOAP annotations and runtime. REST, Kafka, RMI, TCP, and WebSocket remain available as independently configurable transports.

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

The container profile uses PostgreSQL 16 and a bounded HikariCP connection pool. Flyway owns the schema for `sensor_data`, `alerts`, and `recommendations`, including the indexes used by recent-reading and open-alert queries. H2 runs in PostgreSQL compatibility mode for local development and the integration test.

## REST API

Base URI `http://localhost:8080/api/`. `{resourceType}` is one of `water`, `energy`, `traffic`, `waste`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health/live` | Process liveness |
| `GET` | `/health/ready` | Database-backed readiness |
| `GET` | `/dashboard/sensors/{resourceType}?count=10` | Latest sensor readings |
| `GET` | `/dashboard/alerts/{resourceType}` | Active alerts |
| `GET` | `/dashboard/recommendations/{resourceType}` | Generated recommendations |
| `POST` | `/dashboard/alerts/{alertId}/acknowledge` | Acknowledge an alert |
| `POST` | `/dashboard/recommendations/{recommendationId}/implement` | Mark a recommendation as implemented |

Read endpoints hash their payload and expose `X-Data-Hash` and `X-Data-Changed` headers so clients can detect unchanged results.

## Requirements

- Docker with the Compose plugin, or JDK 17 and Maven 3.9+

## Running

Create a local environment file, replace its database password, then build and start the complete stack:

```bash
cp .env.example .env
docker compose up --build
```

The REST API and dashboard are exposed on port 8080; WebSocket traffic uses port 8081. Verify both process and database readiness with:

```bash
curl http://localhost:8080/api/health/live
curl http://localhost:8080/api/health/ready
```

Kafka consumption is enabled in the Compose profile. The simulator and demo seed data remain off until explicitly enabled in `.env`.

Run the tested local profile without containers:

```bash
mvn verify
KAFKA_ENABLED=false WEBSOCKET_ENABLED=false mvn exec:java
```

## Verification

- `mvn clean verify` builds the Java 17 executable JAR and passes the JUnit 5 water-pipeline integration test.
- A packaged-JAR smoke test verifies liveness, database readiness, an empty water-reading query, static dashboard delivery, and graceful shutdown.
- A real WebSocket client handshake verifies the Tyrus endpoint at `/ws`.
- The Compose document is parsed as YAML and references the available official `apache/kafka:3.7.1` image.

Docker is still required to execute the complete PostgreSQL and Kafka container topology end to end.

## Ports

| Port | Service |
| --- | --- |
| 8080 | REST API and static dashboard |
| 8081 | WebSocket endpoint |
| 9092 | Kafka broker (internal to Compose by default) |
| 9999 | Optional sensor TCP socket server |
| 1099 | Optional RMI registry |

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
├── config/                      Environment-driven runtime configuration
├── db/                          Pooled database access
├── services/                    Water, energy, traffic and waste services
├── sensors/                     Sensor simulator
└── simulation/                  Continuous data generator

src/main/resources/db/migration/ Flyway schema migrations
src/test/                        Water pipeline integration test
src/main/webapp/                 Browser dashboard and retained standalone variant
Dockerfile                       Reproducible non-root application image
docker-compose.yml               Application + PostgreSQL + Kafka KRaft
```

## Technologies

| Area | Technologies and demonstrated work |
| --- | --- |
| Runtime and build | Java 17, Maven, Maven Shade executable JAR, scheduled executors, graceful JVM shutdown |
| Event streaming | Apache Kafka 3.7 client, idempotent producer, manual consumer commits, KRaft broker profile |
| HTTP and realtime | JAX-RS, Jersey 2.41, embedded Grizzly, Tyrus WebSocket, liveness and readiness probes |
| Additional transports | Java RMI and a concurrent raw-TCP `ServerSocket` ingestion service |
| Data | PostgreSQL 16, H2 compatibility profile, JDBC, HikariCP pooling, Flyway migrations |
| Serialization and integration | Jackson with Java-time support, Gson, OkHttp, optional external AI recommendation endpoint |
| Observability | SLF4J, Logback, Logstash JSON encoder, structured lifecycle events |
| Dashboard | HTML, CSS, JavaScript, Bootstrap 5, Chart.js, REST polling and WebSocket updates |
| Verification | JUnit 5 integration test covering ingestion, persistence and threshold-based alert creation |
| Packaging | Multi-stage Docker image, non-root runtime, Docker Compose, persistent PostgreSQL and Kafka volumes |

## Runtime configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DB_URL` | local H2 database | JDBC URL; Compose supplies PostgreSQL |
| `DB_USER`, `DB_PASSWORD` | `sa`, blank | Database credentials |
| `DB_MAX_POOL_SIZE` | `10` | Maximum HikariCP pool size |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `PORT` | `8080` | REST API and dashboard port |
| `WEBSOCKET_PORT` | `8081` | WebSocket server port |
| `KAFKA_ENABLED` | `true` | Start the Kafka consumer |
| `WEBSOCKET_ENABLED` | `true` | Start live dashboard push updates |
| `RMI_ENABLED`, `SOCKET_ENABLED` | `false` | Enable the optional RMI and raw-TCP servers |
| `SIMULATOR_ENABLED` | `false` | Enable the multi-transport sensor simulator |
| `CONTINUOUS_GENERATOR_ENABLED` | `false` | Generate readings directly through domain services |
| `DEMO_SEED_DATA` | `false` | Insert a small local demonstration dataset |
| `GEMINI_ENDPOINT`, `GEMINI_API_KEY` | unset | Enable the optional AI recommendation call |
| `STATIC_CONTENT_PATH` | `./src/main/webapp` | Dashboard asset directory |
