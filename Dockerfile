FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD build/libs/transitlog-hfp-csv-sink.jar /usr/app/transitlog-hfp-csv-sink.jar
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=90.0", "-jar", "/usr/app/transitlog-hfp-csv-sink.jar"]
