FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    mkdir -p /opt/jobs

COPY src/ /opt/jobs
WORKDIR /opt/jobs

# Ensure Spark uses the container python
ENV PYSPARK_PYTHON=/usr/bin/python3
