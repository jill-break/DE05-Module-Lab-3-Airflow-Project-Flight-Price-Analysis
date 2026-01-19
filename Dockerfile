# Stage 1: Get Spark
FROM apache/spark:3.5.1-python3 AS spark

# Stage 2: Build Airflow image
FROM apache/airflow:2.8.1-python3.11

USER root

# Copy Spark from the first stage
COPY --from=spark /opt/spark /opt/spark

# Set Spark and Java environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-libmysqlclient-dev \
    openjdk-17-jdk \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/jobs
COPY spark/ /opt/jobs/
RUN chown -R airflow /opt/jobs

# Copy project files and install dependencies
COPY requirements.txt /tmp/requirements.txt

# Switch to the airflow user
USER airflow

RUN pip install --timeout=1200 --no-cache-dir -r /tmp/requirements.txt && \
    pip install --timeout=1200 --no-cache-dir \
    apache-airflow-providers-apache-spark==4.5.0 \
    apache-airflow-providers-mysql==5.5.0 \
    apache-airflow-providers-postgres==5.10.0 \
    mysqlclient==2.2.1 \
    psycopg2-binary==2.9.9