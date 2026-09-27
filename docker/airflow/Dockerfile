# Airflow + Java + PySpark: the daily reconciliation task runs Spark in local mode.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# Resolve JAVA_HOME on both arm64 (Apple Silicon) and amd64 builds.
RUN ln -sfn "$(dirname $(dirname $(readlink -f $(which java))))" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
