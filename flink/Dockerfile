# Base image for Flink
FROM apache/flink:1.20.0

# Switch to Flink directory
WORKDIR /opt/flink

# Install Python and PyFlink dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink confluent-kafka && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy job script, requirements, and dependencies
COPY job/football_stream_job.py /opt/flink/job/
COPY requirements.txt /opt/flink/job/
RUN pip3 install -r /opt/flink/job/requirements.txt

# Copy Kafka JAR into Flink lib directory
COPY lib/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
COPY lib/flink-dist-1.20.0.jar /opt/flink/lib/
COPY lib/kafka-clients-3.9.0.jar /opt/flink/lib/
COPY lib/zstd-jni-1.5.6-9.jar /opt/flink/lib/

# Default Flink ports
EXPOSE 8081 6123 6124

# Start the cluster
CMD ["start-cluster.sh"]
