FROM openjdk:11-jre-slim

RUN apt-get update && apt-get install -qq curl

RUN useradd -m -d /app -s /bin/bash micronaut
USER micronaut
WORKDIR /app

COPY ${project.build.finalName}.jar /app/${project.build.finalName}.jar
COPY lib /app/lib

ENTRYPOINT [ "java", "-XX:+ExitOnOutOfMemoryError", "-noverify", "-jar", "${project.build.finalName}.jar" ]
EXPOSE 8080
