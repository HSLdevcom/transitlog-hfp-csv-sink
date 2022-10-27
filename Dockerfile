FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD build/libs/transitlog-hfp-csv-sink.jar /usr/app/transitlog-hfp-csv-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=50.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitlog-hfp-csv-sink.jar"]
