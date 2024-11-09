# Deployment of an IoT Environmental Monitoring System Utilizing Redpanda Data Streaming and GAMPS Compression Algorithm

This project provides a multi-mode application capable of running as a client, server, Redpanda instance, or server data compression utility. It leverages Kafka-compatible Redpanda for messaging and uses Docker Compose for managing Redpanda instances.

## Prerequisites

- Python 3.x
- Docker and Docker Compose

## Installation

1. **Clone the Repository**

   ```sh
   git clone https://github.com/tuanle186/redpanda_datastreaming_and_compression.git
   cd redpanda_datastreaming_and_compression
   ```

2. **Install Required Python Packages**

   ```sh
   pip install -r requirements.txt
   ```

3. **Download and Place the Dataset**

   Download the dataset from [this link](https://drive.google.com/file/d/1NFk5n-bpwynDvgjb50yT7AkMbWNmAq-h/view) and place it in the appropriate directory. Refer to the [File Structure](#file-structure) section for details.

## Usage

Follow these steps to deploy and run the IoT Environmental Monitoring System:

### Step 1: Start Redpanda

Start Redpanda using Docker Compose:

```sh
python src/main.py redpanda
```

This command initiates a Redpanda instance that acts as the messaging broker.

### Step 2: Run the Client

In a new terminal window, run the client to produce data to the Redpanda broker:

```sh
python src/main.py client --redpanda_ip_address <REDPANDA_IP_ADDRESS>
```

- Replace `<REDPANDA_IP_ADDRESS>` with the IP address of your Redpanda instance.
- If not specified, it defaults to `localhost`.

### Step 3: Run the Server

In another terminal window, run the server to consume data from the Redpanda broker:

```sh
python src/main.py server --redpanda_ip_address <REDPANDA_IP_ADDRESS>
```

- Use the same `<REDPANDA_IP_ADDRESS>` as in the client.
- The server processes the incoming data and applies the GAMPS compression algorithm.

### Step 4: (Optional) Run Server Data Compression

To run the server data compression utility, execute:

```sh
python src/main.py server_data_compression
```

### Step 5: (Optional) Run Server Data Decompression

To run the server data decompression mode:
```sh
python src/main.py server_data_decompression
```
After that enter the number of compressed data entries you want to decompress.

### Step 6: Stop Redpanda

After you are finished, stop Redpanda using Docker Compose:

```sh
python src/main.py stop_redpanda
```

## Configuration

The Kafka configuration is hardcoded in the script for simplicity. You can modify the following parameters in the `main.py` file if necessary:

- `bootstrap.servers`
- `security.protocol`
- `sasl.mechanism`
- `sasl.username`
- `sasl.password`

## File Structure

```
.
├── .gitignore
├── bootstrap.yml
├── config/
├── data/
│   ├── processed/
│   │   ├── data.csv
│   │   ├── sorted/
│   │   │   ├── moteid_1.csv
│   │   │   ├── moteid_2.csv
│   │   │   └── ...
│   │   └── unsorted/
│   │       └── ...
│   └── raw/
│       └── data.txt
├── docker-compose.yml
└── src/
    ├── client_producer/
    │   └── client.py
    ├── main.py
    └── server_consumer/
        ├── data/
        │   ├── compressed/
        │   ├── decompressed/
        │   └── raw_data.txt
        ├── MultiSensorDataGrouper.py
        └── server.py
```

## Acknowledgements

- [Kafka](https://kafka.apache.org/)
- [Docker](https://www.docker.com/)
- [Redpanda](https://redpanda.com/)
