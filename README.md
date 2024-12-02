# Airflow + Spark with PostgreSQL and Adminer

This project sets up a local development environment using Docker for running **Apache Airflow**, **Apache Spark**, **PostgreSQL**, and **Adminer** for managing databases. It includes a Docker Compose configuration for orchestrating these services, along with initialization scripts to set up Airflow, PostgreSQL, and Airflow connections.

## Services

- **PostgreSQL**: Provides a database for Airflow metadata storage.
- **Spark**: Runs a Spark master node.
- **Spark Worker**: Runs Spark worker nodes to process jobs.
- **Airflow**: Runs the Airflow web server and scheduler.
- **Adminer**: Provides a web-based interface for managing PostgreSQL.
- **Airflow Plugin**: Custom resources created with airflow plugin

## Features
- **TimeTable** - To handle schedule only during working days
- **Data persist** - Stores the scraping data and apache spark job outputs inside postgres
- **Adminer** - Adminer setup completed to view and query database

## Prerequisites

Before you begin, ensure you have the following installed on your local machine:

- **Docker** (https://www.docker.com/get-started)
- **Docker Compose** (https://docs.docker.com/compose/)

## Setup and Installation

### 2. Build and Start the Docker Containers

After you have cloned the repository, you need to build and start the containers that define the services in `docker-compose.yml`. Run the following command in your terminal from the root of the project directory:

```bash
docker-compose up --build

````

# Data Engineering Development Environment

## Overview
This project sets up a comprehensive development environment using Docker Compose, integrating Apache Airflow, Apache Spark, PostgreSQL, and Adminer for database management.

## Technologies Used
- **Apache Airflow**: Workflow management platform
- **Apache Spark**: Distributed data processing engine
- **PostgreSQL**: Relational database
- **Adminer**: Database management tool
- **Docker**: Containerization platform






## Environment Setup

### 1. Clone the Repository
```bash
git clone https://github.com/souravmondaldev/movielens-100k-analysis-etl.git
cd movielens-100k-analysis-etl
```

### 2. Build and Start Containers
```bash
# Build all containers
docker-compose build

# Start all services
docker-compose up -d
```

## Access Points

### 1. Airflow Web UI
- **URL**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`
- **Features**:
    - Workflow management
    - DAG monitoring
    - Task execution tracking
    -  Custom Timetable to handle schedule
- ![ui_image](https://i.imgur.com/if0zXSg.png)

### 2. Adminer (Database Management)
- **URL**: `http://localhost:8082`
- **Database**: PostgreSQL
- **Server**: `postgres`
- **Username**: `airflow`
- **Password**: `airflow`
- **Database Name**: `airflow`
- ![db_login_img](https://i.imgur.com/JgM01cr.png)

### 3. Spark Master UI
- **URL**: `http://localhost:7075`
- **Purpose**: Monitor Spark cluster and job execution

## DAG Development

### Location for DAG Scripts
Place your Apache Airflow DAG scripts in:
`./airflow/dags/`

- **pipeline1 -**![imgae_pipeline_1](https://i.imgur.com/5ZV6Oq7.png)
- **pipeline2 -**![imgae_pipeline_2](https://i.imgur.com/3vOcwGo.png)

### Data Storage
- Local data can be placed in: `./airflow/dags/data/`
- Shared with Spark via volume mount

## Debugging and Logs

### View Container Logs
```bash
# View logs for a specific service
docker-compose logs airflow
docker-compose logs spark
docker-compose logs postgres
```

### Stop and Clean Environment
```bash
# Stop all services
docker-compose down

# Remove volumes (careful: will delete data)
docker-compose down -v
```

## Advanced Configuration

### Customizing Environments
Modify environment variables in `docker-compose.yml`:
- Database credentials
- Airflow settings
- Spark configurations

### Adding Python Dependencies
Update `./airflow/requirements.txt

## Resources Used
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Docker Compose Overview](https://docs.docker.com/compose/)

## Video Tutorial Placeholder
🎥 **Walkthrough Video**

https://github.com/user-attachments/assets/824f1c5e-62ca-40e4-b232-84f8a3219dca

- A comprehensive video explaining: [HERE]
    - Environment setup
    - DAG creation
    - Spark job execution
    - Monitoring workflows
