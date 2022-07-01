FROM openjdk:17-slim as builder
WORKDIR /app
COPY . .
RUN ./gradlew bootJar

# TODO: 17-jre-slim
FROM openjdk:17-slim
COPY --from=builder /app/build/libs/grpc-server-0.0.1-SNAPSHOT.jar /app.jar
EXPOSE 8080
EXPOSE 6565
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-Djava.security.egd=file:/dev/./urandom", "-Dserver.port=8080", "-jar", "/app.jar"]
