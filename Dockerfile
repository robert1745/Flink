FROM flink:1.17.1-scala_2.12-java11

# Install Python and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink
RUN pip3 install apache-flink==1.17.1

# Create directory for JAR files
RUN mkdir -p /opt/flink/jars
WORKDIR /opt/flink

# Download Kafka connector
RUN wget -P /opt/flink/jars https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# Copy application files
COPY pyflink_job /opt/flink/pyflink_job

# Create directory for job submission
RUN mkdir -p /opt/flink/usrlib
COPY pyflink_job/kafka_processing.py /opt/flink/usrlib/kafka_processing.py

# Create a directory for JAR files
RUN mkdir -p /opt/flink/usrlib/jars
RUN cp /opt/flink/jars/flink-sql-connector-kafka-1.17.1.jar /opt/flink/usrlib/jars/

# Set environment variables
ENV PYTHONPATH=/opt/flink/usrlib:/opt/flink/usrlib/jars
