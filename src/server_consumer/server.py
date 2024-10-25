import threading
import time
import json
import logging
import numpy as np
import pandas as pd
import os  # Added to use os.fsync()
from confluent_kafka import Consumer, KafkaError
from MultiSensorDataGrouper import MultiSensorDataGrouper, load_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Constants
DATA_FILE = './src/server_consumer/data/sensor_data_output.txt'
COMPRESSED_DATA_DIR = './src/server_consumer/data'
RAW_DATA_FILE = './src/server_consumer/data/sensor_data_output.txt'  # Adjusted to be consistent
TIMEOUT_SECONDS = 5 * 60 * 60  # 5 hours in seconds
KAFKA_POLL_TIMEOUT = 1.0  # Kafka poll timeout in seconds


class Server:
    """
    A server class that consumes sensor data from Kafka, writes it to a file,
    and periodically compresses the data using MultiSensorDataGrouper.
    """

    def __init__(self, kafka_config, topic):
        # Initialize Kafka Consumer
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])

        # Initialize MultiSensorDataGrouper
        self.grouper = MultiSensorDataGrouper(epsilon=2, window_size=100)
        self.base_moteid = 1

        # Event to signal thread termination
        self.stop_event = threading.Event()

    def consume_data(self):
        """
        Consume data from Kafka and write it to a file.
        """
        try:
            with open(DATA_FILE, 'a') as file:
                while not self.stop_event.is_set():
                    msg = self.consumer.poll(timeout=KAFKA_POLL_TIMEOUT)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logging.error(f"Kafka error: {msg.error()}")
                            break
                    else:
                        try:
                            # Decode the message
                            message_value = msg.value().decode('utf-8')
                            data = json.loads(message_value)
                            logging.info(f"Received data: {data}")

                            # Write data to the output file
                            file.write(
                                f"{data['date']} {data['time']} {data['epoch']} "
                                f"{data['moteid']} {data['temperature']} "
                                f"{data['humidity']} {data['light']} {data['voltage']}\n"
                            )
                            # Flush the buffer to ensure data is written to disk
                            file.flush()
                            os.fsync(file.fileno())
                        except json.JSONDecodeError:
                            logging.warning(f"Received non-JSON message: {message_value}")
        finally:
            self.consumer.close()

    def compress_data(self):
        """
        Periodically compress data using MultiSensorDataGrouper.
        """
        try:
            while not self.stop_event.is_set():
                # Wait for the specified timeout or until stop_event is set
                self.stop_event.wait(timeout=TIMEOUT_SECONDS)

                if self.stop_event.is_set():
                    break

                self.compress_data_helper()
        except Exception as e:
            logging.error(f"Error in compress_data: {e}")

    def compress_data_helper(self):
        """
        Helper function to compress data and write compressed data to files.
        """
        try:
            # Load data for compression
            df = load_data(RAW_DATA_FILE)  # Ensure this function returns a pandas DataFrame

            # List of attributes to process
            attributes = ['temperature', 'humidity', 'light', 'voltage']

            # Dictionary to store compressed data for each attribute
            compressed_data = {}

            for attribute in attributes:
                # Extract signals
                base_signal, other_signals, timestamps = self.grouper.extract_signals(
                    df, self.base_moteid, attribute
                )

                # Perform static grouping to get compression buckets
                base_signals, ratio_signals, total_memory_cost = self.grouper.static_group(
                    df, self.base_moteid, attribute
                )

                # Reconstruct the compressed signals
                reconstructed_base_signal = self.grouper.reconstruct_signal(base_signals[0][1])

                reconstructed_other_signals = {}
                for moteid, ratio_buckets in ratio_signals.items():
                    reconstructed_signal = self.grouper.reconstruct_signal(
                        ratio_buckets, reconstructed_base_signal
                    )
                    reconstructed_other_signals[moteid] = reconstructed_signal

                # Store the compressed and reconstructed data
                compressed_data[attribute] = {
                    'original_signals': [base_signal] + other_signals,
                    'reconstructed_signals': [reconstructed_base_signal] + list(reconstructed_other_signals.values()),
                    'timestamps': timestamps,
                    'total_memory_cost': total_memory_cost,
                }

                # Output the total memory cost
                logging.info(f'Attribute: {attribute}')
                logging.info(f'Total memory cost after compression: {total_memory_cost} buckets')

                # Convert signals to JSON-serializable formats
                processed_base_signals = [
                    [int(moteid), self.convert_numpy_types(signal_data)]
                    for moteid, signal_data in base_signals
                ]

                processed_ratio_signals = {
                    str(int(moteid)): self.convert_numpy_types(signal_data)
                    for moteid, signal_data in ratio_signals.items()
                }

                # Write the compressed data to files
                compressed_file = f'{COMPRESSED_DATA_DIR}/compressed_{attribute}_data.txt'
                with open(compressed_file, 'w') as file:
                    json.dump({
                        'base_signals': processed_base_signals,
                        'ratio_signals': processed_ratio_signals,
                        'total_memory_cost': total_memory_cost
                    }, file)
                    logging.info(f'Compressed data written to {compressed_file}')

                # Write the reconstructed data to files
                reconstructed_file = f'{COMPRESSED_DATA_DIR}/reconstructed_{attribute}_data.txt'
                with open(reconstructed_file, 'w') as file:
                    for moteid, signal in zip(
                        [self.base_moteid] + list(ratio_signals.keys()),
                        compressed_data[attribute]['reconstructed_signals']
                    ):
                        json.dump({
                            'moteid': int(moteid),
                            'attribute': attribute,
                            'signal': self.convert_numpy_types(signal),
                            'timestamps': self.convert_numpy_types(timestamps)
                        }, file)
                        file.write('\n')
                    logging.info(f'Reconstructed data written to {reconstructed_file}')

        except Exception as e:
            logging.error(f"Error in compress_data_helper: {e}")

    @staticmethod
    def convert_numpy_types(obj):
        """
        Convert numpy data types to native Python data types for JSON serialization.
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, dict):
            return {Server.convert_numpy_types(k): Server.convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [Server.convert_numpy_types(item) for item in obj]
        else:
            return obj

    def run(self):
        """
        Run the server by starting the consumer and compression threads.
        """
        # Start the consumer thread
        consumer_thread = threading.Thread(target=self.consume_data)
        consumer_thread.start()

        # Start the compression thread
        compression_thread = threading.Thread(target=self.compress_data)
        compression_thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            # Signal threads to stop
            self.stop_event.set()
            consumer_thread.join()
            compression_thread.join()


if __name__ == "__main__":
    kafka_conf = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }

    topic = 'sensor_data'

    server = Server(kafka_conf, topic)
    server.run()
