# build context 는 프로젝트 루트다 (Gradle 서브모듈이라 루트의 wrapper·settings 가 필요하다).
#   docker compose up -d --build mock-pg
FROM eclipse-temurin:24-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew :mock-pg:shadowJar --no-daemon

FROM eclipse-temurin:24-jre
WORKDIR /app
COPY --from=build /src/mock-pg/build/libs/*-all.jar mock-pg.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/mock-pg.jar"]
