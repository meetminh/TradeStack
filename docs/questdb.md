# QuestDB Docker Setup: Comprehensive Guide

This document provides a detailed guide to setting up, running, and managing a QuestDB Docker container with data stored on an external SSD. It includes step-by-step instructions, explanations, best practices, and advanced considerations for developers.

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup](#setup)
   - [Connecting the External SSD](#connecting-the-external-ssd)
   - [Creating the Data Directory](#creating-the-data-directory)
3. [Running the QuestDB Container](#running-the-questdb-container)
   - [Docker Run Command](#docker-run-command)
   - [Ports Explained](#ports-explained)
   - [Volume Mount Explained](#volume-mount-explained)
4. [Stopping the QuestDB Container](#stopping-the-questdb-container)
5. [Advanced Usage](#advanced-usage)
   - [Running in Detached Mode](#running-in-detached-mode)
   - [Using Docker Compose](#using-docker-compose)
   - [Environment Variables](#environment-variables)
6. [What’s Possible?](#whats-possible)
   - [Portability](#portability)
   - [Data Persistence](#data-persistence)
   - [Multi-Platform Compatibility](#multi-platform-compatibility)
   - [Scalability](#scalability)
   - [Development and Testing](#development-and-testing)
7. [Troubleshooting](#troubleshooting)
   - [Permission Issues](#permission-issues)
   - [File System Compatibility](#file-system-compatibility)
   - [Container Fails to Start](#container-fails-to-start)
8. [Example Commands for Different OS](#example-commands-for-different-os)
   - [macOS](#macos)
   - [Linux](#linux)
   - [Windows](#windows)
9. [Best Practices](#best-practices)
10. [Conclusion](#conclusion)

---

## Prerequisites

Before starting, ensure the following are in place:

1. **Docker Installed**:
   - Install Docker from [https://www.docker.com/get-started](https://www.docker.com/get-started).
   - Verify the installation by running:
     ```bash
     docker --version
     ```
   - If Docker is not installed, follow the official installation guide for your operating system.

2. **External SSD**:
   - Use an external SSD with sufficient storage for your QuestDB data.
   - Format the SSD with a compatible file system:
     - **exFAT**: Cross-platform compatibility (macOS, Windows, Linux).
     - **APFS/HFS+**: macOS-only.
     - **NTFS**: Windows-only.
     - **ext4**: Linux-only.

3. **QuestDB Docker Image**:
   - Pull the QuestDB Docker image:
     ```bash
     docker pull questdb/questdb:8.2.1
     ```

---

## Setup

### Connecting the External SSD
1. Plug the external SSD into your laptop.
2. Verify the SSD is mounted:
   - **macOS**: Open `Finder` and check under `/Volumes/`.
   - **Linux**: Use the `lsblk` or `df -h` command to locate the SSD.
   - **Windows**: Open `File Explorer` and check the drive letter (e.g., `D:`).

### Creating the Data Directory
1. Create a directory on the SSD for QuestDB data:
   ```bash
   mkdir -p /Volumes/ssd1/questdb-data

   Ensure the directory has the correct permissions:
bash
Copy
chmod -R 777 /Volumes/ssd1/questdb-data
This ensures Docker can read/write to the directory.
Running the QuestDB Container

Docker Run Command

Use the following command to start the QuestDB container:

bash
Copy

docker run \
  -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 \
  -v /Volumes/ssd1/questdb-data:/var/lib/questdb \
  questdb/questdb:8.2.1


### Ports Explained

9000: QuestDB Web Console (access via http://localhost:9000).
9009: InfluxDB Line Protocol (for time-series data ingestion).
8812: PostgreSQL wire protocol (for SQL queries).
9003: REST API (for programmatic access).
Volume Mount Explained

/Volumes/ssd1/questdb-data: The path to the external SSD on your host machine.
/var/lib/questdb: The path inside the container where QuestDB stores its data.
Stopping the QuestDB Container

### To stop the container gracefully:

Find the container ID:
bash
Copy
docker ps
This lists all running containers and their IDs.
Stop the container:
bash
Copy
docker stop <container_id>
Replace <container_id> with the actual container ID.
Advanced Usage

Running in Detached Mode

To run the container in the background, add the -d flag:

bash
Copy
## Run Container again on new system:
docker run -d \
  -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 \
  -v /path_to_questDB_folder:/var/lib/questdb \
  questdb/questdb:8.2.1


docker run -d \
  -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 \
  -v /Volumes/ssd1/questdb-data:/var/lib/questdb \
  questdb/questdb:8.2.1


Using Docker Compose

Create a docker-compose.yml file for easier management:

yaml
Copy
version: '3.8'
services:
  questdb:
    image: questdb/questdb:8.2.1
    ports:
      - "9000:9000"
      - "9009:9009"
      - "8812:8812"
      - "9003:9003"
    volumes:
      - /Volumes/ssd1/questdb-data:/var/lib/questdb
Run the container:

bash
Copy
docker-compose up -d
Environment Variables

Set environment variables for configuration:

bash
Copy


docker run -d \
  -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 \
  -v /Volumes/ssd1/questdb-data:/var/lib/questdb \
  -e QDB_METRICS_ENABLED=true \
  questdb/questdb:8.2.1