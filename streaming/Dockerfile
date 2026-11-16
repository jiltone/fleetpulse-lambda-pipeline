# Spark 3.5 (Scala 2.12, Python 3) + the Kafka connector jars + psycopg2.
FROM apache/spark:3.5.3-python3

USER root

# Kafka source/sink for Structured Streaming and its dependencies. Downloading them
# at build time means the container does not need internet access at runtime.
ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

COPY streaming/requirements.txt /tmp/requirements.txt
RUN chmod 644 /opt/spark/jars/*.jar \
 && pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY common/ /app/common/
COPY streaming/ /app/streaming/

# Runs as root so it can write checkpoints/lake files to the Windows bind mount (./data).
