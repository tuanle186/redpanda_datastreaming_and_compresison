# Deployment of an IoT Environmental Monitoring System Utilizing RedPanda Data Streaming and GAMPS Compression Algorithm

This project provides a multi-mode application that can run as a client, server, Redpanda instance, or server data compression utility. The application uses Kafka for messaging and Docker Compose for managing Redpanda.

## Prerequisites

- Python 3.x
- Docker and Docker Compose

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/tuanle186/redpanda_datastreaming_and_compresison
    cd redpanda_data_streaming_and_compression
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```
3. Download the dataset and put in the correct directory (Read the [File Structure section at the end of this file](#File-Structure)  for reference.
   Dataset link: https://drive.google.com/file/d/1NFk5n-bpwynDvgjb50yT7AkMbWNmAq-h/view

## Usage

The application can be run in different modes: 

`client`, `server`, `redpanda`, and `server_data_compression`. The `--ip_address` if not specified will be set to `localhost` by default.

### Running the Client

To run the client mode:
```sh
python src/main.py client --ip_address <IP_ADDRESS>
```

### Running the Server

To run the server mode:
```sh
python src/main.py server --ip_address <IP_ADDRESS>
```

### Running Redpanda

To start Redpanda using Docker Compose:
```sh
python src/main.py redpanda --ip_address <IP_ADDRESS>
```

### Running Server Data Compression

To run the server data compression mode:
```sh
python src/main.py server_data_compression
```

## Configuration

The Kafka configuration is hardcoded in the script for simplicity. You can modify the following parameters in the 

main.py

 file:

- `bootstrap.servers`
- `security.protocol`
- `sasl.mechanism`
- `sasl.username`
- `sasl.password`

## File Structure

```
.gitignore
bootstrap.yml
config/
data/
    processed/
        .ipynb_checkpoints/
            data-checkpoint.csv
        data.csv
        sorted/
            .ipynb_checkpoints/
            moteid_1.csv
            moteid_10.csv
            moteid_11.csv
            moteid_12.csv
            moteid_13.csv
            moteid_14.csv
            moteid_15.csv
            moteid_16.csv
            moteid_17.csv
            ...
        unsorted/
            ...
    raw/
        data.txt
docker-compose.yml
src/
    client_producer/
        __pycache__/
        client.py
    main.py
    server_consumer/
        __pycache__/
        data/
            ...
        MultiSensorDataGrouper.py
        server.py
```

## Acknowledgements

- [Kafka](https://kafka.apache.org/)
- [Docker](https://www.docker.com/)
- [Redpanda](https://vectorized.io/redpanda/)
