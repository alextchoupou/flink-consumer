# Base image
FROM openjdk:11-jdk-slim AS build

# Working directory
WORKDIR /app

# Copy Mavev files and source code to the container
COPY pom.xml ./
COPY src ./src

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Construction step
RUN --mount=type=cache,target=/root/.m2 mvn -B clean package -DskipTests

# Create a new image for execution
FROM openjdk:11-jdk-slim

WORKDIR /app

# Copy the JAR file from the build stage
COPY --from=build /app/target/FlinkConsumer-1.0-SNAPSHOT.jar /shared/app.jar

# Copy the Flink JAR
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /app/flink-dist.jar

# Installer curl pour vérifier Elasticsearch et récupérer le certificat
# RUN apt-get update && \
#    apt-get install -y curl && \
#    apt-get clean

# Commnd to run the application with the Flink JAR
# ENTRYPOINT ["java", "-cp", "/shared/app.jar:/app/flink-dist.jar", "group.ventis.DataStreamJob"]