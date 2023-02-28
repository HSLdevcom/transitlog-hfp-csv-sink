FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD build/libs/transitlog-hfp-csv-sink.jar /usr/app/transitlog-hfp-csv-sink.jar
ADD start-application.sh /usr/app/start-application.sh
ENTRYPOINT ["/usr/app/start-application.sh"]
