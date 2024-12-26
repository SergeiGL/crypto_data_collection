# Crypto Data Collection

A robust data collection system that parses and stores cryptocurrency exchange data in SQL databases.


## Overview

This system is designed to:
- Parse real-time data from various cryptocurrency exchanges
- Store parsed data in SQL databases
- Provide notification capabilities via Telegram

## Features

- Multi-exchange support:
  - Binance (spot and futures)
  - OKX futures
  - Bybit futures
  - Deribit futures
  - Vertex Protocol
  - DYDX
- Websocket connections for real-time data
- Queue-based processing system
- Automated parsing management through shell scripts
- Containerized deployment with Docker

## Requirements

### Configuration Files

The following configuration files need to be present in your local directory:

1. `sql_config.py`:
```python
DB_CONFIG = {
    "user": "",
    "port": "",
    "password": "",
    "host": "",
    "db": ""
}
```

2. `tg_bot_config.py`:
```python
TG_TOKEN = ''
TG_CHAT_ID_ERRORS = ''
TG_TOKEN_MESSAGES = ''
TG_CHAT_ID_MESSAGES = ''
```

3. `local_settings.py` (for additional configuration)

## Docker Setup

The project uses a Python 3.11.4 slim-buster base image with the following configurations:
- Timezone set to Europe/Moscow
- Automatic installation of dependencies from requirements.txt
- Volume mounting for configuration files
- Automatic container restart unless explicitly stopped

## Usage

### Building the Docker Image

```bash
docker build -t parse .
```

### Running Parsers

To run all parsers defined in script_list.txt:
```bash
./parse_all.sh
```
This script reads exchange names from script_list.txt and launches parsers for each one.

### Volume Mounts

The following files are mounted into the container:
- sql_config.py
- tg_bot_config.py
- local_settings.py

### Container Management

Each parser runs in its own container named after the exchange it's parsing. Standard Docker commands can be used for management:

```bash
# View running containers
docker ps

# View container logs
docker logs <container_name>

# Stop a parser
docker stop <container_name>

# Remove a container
docker rm <container_name>
```


Enjoy programming!
