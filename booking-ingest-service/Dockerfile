# syntax=docker/dockerfile:1
#
# Identical for all three services. Copy this file (and .dockerignore) into
# gateway-service/, booking-ingest-service/ and booking-worker-service/.

# ---------- build ----------
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Dependencies resolve in their own layer. Docker caches it and only invalidates it when
# pom.xml changes, so an ordinary code change rebuilds in seconds instead of re-downloading
# every dependency. Copying src first would throw that cache away on every commit.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q clean package -DskipTests

# Tests are skipped here on purpose: CI already ran them against real containers before this
# image is ever built. Running them again inside the image build would need a Docker daemon
# inside the build, double the build time, and prove nothing new.

# ---------- runtime ----------
FROM eclipse-temurin:21-jre-alpine AS runtime

# Never run as root. A container escape from an unprivileged process is a far smaller problem.
RUN addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
USER app

# MaxRAMPercentage makes the JVM size its heap from the CONTAINER's memory limit rather than
# the host's. Without it, a JVM in a 512 MB container on an 8 GB host sizes its heap for 8 GB
# and gets OOM-killed under load — a failure that looks like a crash and is really a config
# mistake.
#
# Deliberately NOT using -XX:+UseSerialGC or -XX:TieredStopAtLevel=1. Those are the standard
# "small container" flags and they trade throughput for startup time and footprint, which is
# exactly backwards for a service whose entire purpose is sustained concurrent throughput.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70"

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
