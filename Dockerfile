FROM  apache/spark:3.5.1-python3
USER root

# Install python + pip if not present
RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Jobs dir
RUN mkdir -p /opt/jobs

COPY src/ /opt/jobs

WORKDIR /opt/jobs
