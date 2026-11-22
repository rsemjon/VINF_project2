FROM python:3.12-slim-bookworm

ARG SPARK_VERSION=4.0.1
ARG HADOOP_PROFILE=3

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz" \
    | tar -xz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}" "${SPARK_HOME}"

WORKDIR /app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark/bin/pyspark"]
