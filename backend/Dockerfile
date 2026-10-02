# syntax=docker/dockerfile:1
# Spring Boot backend image. Built by infra/docker-compose.yml (profile "app"), or standalone:
#   docker build -t kafka-demo/backend:local backend

# ---- build ------------------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
# The cache mount keeps ~/.m2 between builds, so only changed dependencies are downloaded.
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests \
 && java -Djarmode=tools -jar target/*.jar extract --layers --launcher --destination /extracted

# ---- runtime ----------------------------------------------------------------------------------
FROM eclipse-temurin:17-jre
RUN groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app

# Layers ordered from least to most frequently changed, so a code change only rebuilds the last one.
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./

USER app
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

# No curl/wget in the base image: bash's /dev/tcp does a plain HTTP GET instead.
HEALTHCHECK --interval=10s --timeout=5s --start-period=40s --retries=5 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080 && printf "GET /actuator/health HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3 && grep -q "\"status\":\"UP\"" <&3'

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
