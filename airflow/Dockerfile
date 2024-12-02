# # Base image
# FROM apache/airflow:2.7.3

# # Set environment variables
# ENV AIRFLOW_HOME=/opt/airflow

# # Switch to 'airflow' user for installing dependencies
# USER airflow

# # Install dependencies for PostgreSQL and Spark
# #RUN pip install --no-cache-dir psycopg2-binary
# #RUN pip install --no-cache-dir pyspark

# # Copy requirements file and install dependencies
# COPY requirements.txt /requirements.txt

# # Install the required Python packages from requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt

# # Copy local DAGs to container
# COPY ./dags $AIRFLOW_HOME/dags

# # Ensure we continue to use the 'airflow' user for running Airflow
# USER airflow

FROM apache/airflow:2.7.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     openjdk-17-jdk \
#     procps \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# ENV PATH=$PATH:$JAVA_HOME/bin
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget\
    procps
RUN wget https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u292-b10/OpenJDK8U-jdk_aarch64_linux_hotspot_8u292b10.tar.gz -O /tmp/openjdk8.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xzf /tmp/openjdk8.tar.gz -C /usr/lib/jvm && \
    rm /tmp/openjdk8.tar.gz

# Set JAVA_HOME and PATH
ENV JAVA_HOME=/usr/lib/jvm/jdk8u292-b10
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt