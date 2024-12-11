import argparse
import subprocess
from client_producer.client import Client
from server_consumer.server import Server
import os
import webbrowser
import socket

def run_client(redpanda_ip_address):
    """
    Run the client to produce data to the Kafka topic.

    Args:
        ip_address (str): The IP address of the Kafka bootstrap server.
    """
    kafka_conf = {
        'bootstrap.servers': f'{redpanda_ip_address}:19092',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }
    schema_registry_url = 'http://' + f'{redpanda_ip_address}:18081'
    data_file = 'data/processed/data.csv'
    client = Client(data_file,schema_registry_url)
    try:
        client.connect(kafka_conf)
        client.run()
    except Exception as e:
        print(f"An error occurred in the client: {e}")


def run_server(redpanda_ip_address):
    """
    Run the server to consume data from the Kafka topic.

    Args:
        ip_address (str): The IP address of the Kafka bootstrap server.
    """
    kafka_conf = {
        'bootstrap.servers': f'{redpanda_ip_address}:19092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }
    schema_registry_url = 'http://' + f'{redpanda_ip_address}:18081'
    
    raw_data_path = './src/server_consumer/data/raw_data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    decompressed_data_path = './src/server_consumer/data/decompressed'
    server = Server(raw_data_path, compressed_data_path,decompressed_data_path,schema_registry_url)
    try:
        server.connect(kafka_conf)
        server.run()
    except Exception as e:
        print(f"An error occurred in the server: {e}")


def run_server_data_compression():
    """
    Run the server to compress and store data.

    """
    raw_data_path = './data/raw/data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    decompressed_data_path = './src/server_consumer/data/decompressed'
    server = Server(raw_data_path, compressed_data_path,decompressed_data_path)
    try:
        server.compress_and_store()
    except Exception as e:
        print(f"An error occurred in the server: {e}")

def run_server_data_decompression():
    """
    Run the server to compress and store data.
    """
    raw_data_path = './data/raw/data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    decompressed_data_path = './src/server_consumer/data/decompressed'
    server = Server(raw_data_path, compressed_data_path,decompressed_data_path)
    counter_file = os.path.join(compressed_data_path, 'compression_counter.txt')
    try:
        if os.path.exists(counter_file):
            with open(counter_file, 'r') as f:
                max_compression_count = int(f.read().strip())
        else:
            print("No compressed data available for decompression.")
            return

        while True:
            try:
                num_to_decompress = int(input(f"Enter the number of compressed data entries you want to decompress (1 to {max_compression_count}): "))
                if 1 <= num_to_decompress <= max_compression_count:
                    break  
                else:
                    print(f"Please enter a number between 1 and {max_compression_count}.")
            except ValueError:
                print("Invalid input. Please enter a valid number.")

        server.decompress_and_store(num_to_decompress)

    except Exception as e:
        print(f"An error occurred in the server: {e}")
        
def run_redpanda():
    """
    Run Redpanda using Docker Compose and open the web UI in a browser.
    """
    try:
        # Get the host's IP address
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)

        print("Starting Redpanda using Docker Compose...")
        os.environ['PUBLIC_IP'] = ip_address
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print(f"Redpanda is running on IP address {ip_address} and port 8080")
        print(f"Opening Redpanda Console at http://{ip_address}:8080...")
        webbrowser.open(f"http://{ip_address}:8080")
    except subprocess.CalledProcessError as e:
        print(f"Failed to start Redpanda with Docker Compose: {e}")
    except socket.error as e:
        print(f"Failed to get the host IP address: {e}")


def stop_redpanda():
    """
    Stop Redpanda using Docker Compose.
    """
    try:
        print("Stopping Redpanda using Docker Compose...")
        subprocess.run(["docker-compose", "down"], check=True)
        print("Redpanda has been stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop Redpanda with Docker Compose: {e}")


def main(mode, ip_address):
    # Initialize based on the mode
    if mode == "client":
        print("Running in CLIENT mode...")
        run_client(ip_address)

    elif mode == "server":
        print("Running in SERVER mode...")
        run_server(ip_address)

    elif mode == "redpanda":
        print("Running in REDPANDA mode...")
        run_redpanda()

    elif mode == "stop_redpanda":
        print("Running in STOP_REDPANDA mode...")
        stop_redpanda()

    elif mode == "server_data_compression":
        print("Running in SERVER DATA COMPRESSION mode...")
        run_server_data_compression()
        
    elif mode == "server_data_decompression":
        print("Running in SERVER DATA DECOMPRESSION mode...")
        run_server_data_decompression()

    else:
        print("Invalid mode selected. Please choose a valid mode: client, server, redpanda, server_data_compression, server_data_decompression.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the application in different modes")
    parser.add_argument("mode", type=str, choices=["client", "server", "redpanda", "stop_redpanda", "server_data_compression","server_data_decompression"], 
                        help="Mode to run the script in (client, server, redpanda, stop_redpanda, server_data_compression,server_data_decompression)")
    parser.add_argument("--redpanda_ip_address", type=str, default="localhost", help="IP address of the redpanda server (default: localhost)")
    args = parser.parse_args()
    main(args.mode, args.redpanda_ip_address)
