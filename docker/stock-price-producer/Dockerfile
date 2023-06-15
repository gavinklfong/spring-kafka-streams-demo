FROM openjdk:18-jdk-alpine
COPY stock-price-producer/target/stock-price-producer.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]