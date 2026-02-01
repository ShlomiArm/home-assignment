# Apache Spark with Java 11 + Python
FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

# Install pip (usually present, but ensure)
RUN apt-get update \
  && apt-get install -y python3-pip \
  && rm -rf /var/lib/apt/lists/*

# App directory
WORKDIR /opt/app

# Install Python dependencies
COPY requirements.txt /opt/app/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Fix Ivy cache issue (/nonexistent/.ivy2)
RUN mkdir -p /opt/ivy \
  && chmod -R 777 /opt/ivy

# Optional: copy source (you can also mount at runtime)
COPY src/ /opt/app/src/

# Spark runs as user "spark"
USER spark

# IMPORTANT: set HOME to writable location
ENV HOME=/opt/ivy
