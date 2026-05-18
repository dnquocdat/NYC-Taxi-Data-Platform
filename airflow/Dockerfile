ARG AIRFLOW_IMAGE_TAG=3.1.0
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-docker>=4.4.0" \
    "pydantic>=2.7" \
    "PyYAML>=6.0" \
    "requests>=2.32" \
    "delta-spark==3.2.0" \
    "pyarrow>=15.0" \
    "pyspark==3.5.1"
