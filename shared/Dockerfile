FROM openjdk:8-jre-alpine
COPY libs/ /
ARG JAR_NAME
RUN ln -s ${JAR_NAME} app.jar
ENTRYPOINT ["java","-jar","app.jar"]