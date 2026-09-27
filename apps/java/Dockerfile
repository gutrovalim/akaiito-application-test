# syntax=docker/dockerfile:1
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:copy@tracer-agents
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package

FROM eclipse-temurin:21.0.8_9-jre
COPY --from=build /src/target/agents/ /opt/akt/agents/
COPY --from=build /src/target/akt-app.jar /opt/akt/app.jar
COPY entrypoint.sh /opt/akt/entrypoint.sh
ENTRYPOINT ["/opt/akt/entrypoint.sh"]
