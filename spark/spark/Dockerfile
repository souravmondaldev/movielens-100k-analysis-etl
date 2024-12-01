FROM bitnami/spark:latest

# Set correct Spark environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
USER root

# Download and copy PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y wget
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -o /opt/bitnami/spark/jars/postgresql-42.2.23.jar

# Copy the Spark job script
COPY app/spark_job.py /app/spark_job.py
