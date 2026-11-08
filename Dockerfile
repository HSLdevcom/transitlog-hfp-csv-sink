# syntax=docker/dockerfile:1

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS build
WORKDIR /usr/app
COPY . .
RUN --mount=type=secret,id=github_token,env=GITHUB_TOKEN \
    --mount=type=secret,id=github_actor,env=GITHUB_ACTOR \
    ./gradlew --no-daemon shadowJar

FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre
WORKDIR /usr/app
COPY --from=build /usr/app/build/libs/transitlog-hfp-csv-sink.jar /usr/app/transitlog-hfp-csv-sink.jar
RUN chown 10001:10001 /usr/app
USER 10001:10001
ENTRYPOINT ["java", "-XX:InitialRAMPercentage=10.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitlog-hfp-csv-sink.jar"]
