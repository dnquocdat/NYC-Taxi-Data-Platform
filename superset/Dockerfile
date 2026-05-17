ARG SUPERSET_IMAGE_TAG=4.1.2
FROM apache/superset:${SUPERSET_IMAGE_TAG}

USER root

RUN python -m pip install --no-cache-dir "clickhouse-connect>=0.6.8"

USER superset
