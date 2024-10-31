import argparse
import multiprocessing
from client_producer.client import Client
from server_consumer.server import Server
# from redpanda_module.redpanda import Redpanda  # Uncomment if Redpanda is a separate module

def run_client():
    kafka_conf = {
        'bootstrap.servers': 'localhost:19092',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }

    data_file = 'data/processed/data.csv'
    client = Client(data_file)
    try:
        client.connect(kafka_conf)
        client.run()
    except Exception as e:
        print(f"An error occurred in the client: {e}")

def run_server():
    kafka_conf = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }

    raw_data_path = './src/server_consumer/data/raw_data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    server = Server(raw_data_path, compressed_data_path)
    try:
        server.connect(kafka_conf)
        server.run()
    except Exception as e:
        print(f"An error occurred in the server: {e}")

def run_server_data_compression():
    raw_data_path = './data/raw/data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    server = Server(raw_data_path, compressed_data_path)
    try:
        server.compress_and_store()
    except Exception as e:
        print(f"An error occurred in the server: {e}")


def main(mode):
    # Initialize based on the mode
    if mode == 1:
        print("Running in CLIENT mode...")
        run_client()

    elif mode == 2:
        print("Running in SERVER mode...")
        run_server()

    elif mode == 3:
        print("Running in SERVER DATA COMPRESSION mode...")
        run_server_data_compression()

    elif mode == 4:
        print("Running in CLIENT and SERVER mode (concurrent)...")
        client_process = multiprocessing.Process(target=run_client)
        server_process = multiprocessing.Process(target=run_server)

        client_process.start()
        server_process.start()

        client_process.join()
        server_process.join()

    elif mode == 5:
        print("Running in CLIENT, SERVER, and REDPANDA mode...")
        client_process = multiprocessing.Process(target=run_client)
        server_process = multiprocessing.Process(target=run_server)
        # Uncomment and adjust if Redpanda is available
        # redpanda = Redpanda()
        # redpanda_process = multiprocessing.Process(target=redpanda.start)

        client_process.start()
        server_process.start()
        # redpanda_process.start()

        client_process.join()
        server_process.join()
        # redpanda_process.join()

    else:
        print("Invalid mode selected. Please choose a valid mode (1-5).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the application in different modes")
    parser.add_argument("mode", type=int, choices=range(1, 6), help="Mode to run the script in (1-5)")
    args = parser.parse_args()
    main(args.mode)
