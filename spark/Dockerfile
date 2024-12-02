FROM bitnami/spark

USER root

# Install wget and other utilities
RUN apt-get update && apt-get install -y wget curl rsync procps ca-certificates gnupg2
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    libbz2-dev \
    liblzma-dev \
    tk-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    curl \
    libbz2-dev \
    && apt-get clean

# Download and install Python 3.8 from source
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz && \
    tar -xvf Python-3.8.10.tgz && \
    cd Python-3.8.10 && \
    ./configure --enable-optimizations && \
    make -j 4 && \
    make altinstall && \
    rm -rf /tmp/Python-3.8.10 /tmp/Python-3.8.10.tgz

# Set python3.8 as the default python
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.8 1

# Install pip for Python 3.8
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3.8 get-pip.py && \
    rm get-pip.py

# Install required Python packages
RUN python3.8 -m pip install pyspark
# Install OpenJDK 11
# RUN mkdir -p /usr/share/man/man1 && \
#     apt-get update && \
#     apt-get install -y openjdk-17-jdk && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# ENV PATH=$PATH:$JAVA_HOME/bin

# Set correct Spark environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and copy PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -O $SPARK_HOME/jars/postgresql-42.2.23.jar

# Copy the Spark job script
COPY app/ /app/

USER 1001

CMD ["bash"]