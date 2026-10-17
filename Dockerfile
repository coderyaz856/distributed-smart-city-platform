FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B verify

FROM eclipse-temurin:17-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system smartcity \
    && useradd --system --gid smartcity --home /app smartcity
WORKDIR /app
COPY --from=build /workspace/target/smartcity.jar /app/smartcity.jar
COPY --from=build /workspace/src/main/webapp /app/webapp
USER smartcity
ENV STATIC_CONTENT_PATH=/app/webapp
EXPOSE 8080 8081
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=4 \
  CMD curl --fail --silent http://localhost:${PORT:-8080}/api/health/ready || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/smartcity.jar"]
