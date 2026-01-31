FROM  apache/spark:3.5.1-python3

# Install python + pip if not present
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    mkdir -p /opt/jobs

COPY src/ /opt/jobs

WORKDIR /opt/jobs
