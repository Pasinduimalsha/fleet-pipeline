# Spark Structured Streaming speed layer, run via `spark-submit --master local[*]`.
# local[*] is sufficient at this data volume; see README for the scale-out note.
#
# Bitnami's Spark images were pulled from the free Docker Hub tier in 2025,
# so this uses the official Apache Spark image instead.
FROM apache/spark:3.5.3-python3
USER root
ENV PATH="/opt/spark/bin:${PATH}"
# Image ships Python 3.8 -- pandas/pyarrow pinned to versions that still
# support it (newer pandas/pyarrow require 3.9+).
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    structlog==24.4.0 \
    prometheus-client==0.21.0 \
    kafka-python==2.0.2 \
    pandas==2.0.3 \
    pyarrow==14.0.2
