import threading
import time
import json
import logging
import numpy as np
import os
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import Dict, Optional
from server_consumer.MultiSensorDataGrouper import MultiSensorDataGrouper, load_data
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class Server:
    """
    A server class that consumes sensor data from Kafka, writes it to a file,
    and periodically compresses the data using MultiSensorDataGrouper.
    """

    def __init__(self, raw_data_path: str, compressed_data_path: str ,decompressed_data_path: str, schema_registry_url: Optional[str] = None):
        self.topic = 'sensor-data'
        self.consumer: Optional[Consumer] = None
        self.data_compress_interval = 5 * 60 * 60  # 5 hours
        self.raw_data_path = raw_data_path
        self.compressed_data_path = compressed_data_path
        self.decompressed_data_path = decompressed_data_path
        self.grouper = MultiSensorDataGrouper(epsilon=2, window_size=100)
        self.base_moteid = 1
        self.stop_event = threading.Event()
        self.attributes = ['temperature', 'humidity', 'light', 'voltage']
        
        self.schema_registry_url = schema_registry_url

        if self.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
            self.avro_deserializer = AvroDeserializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=None
            )
        else:
            self.schema_registry_client = None
            self.avro_deserializer = None
            # logging.warning("Schema Registry URL not provided.")

        
    def connect(self, kafka_config: Dict[str, str]):
        """
        Connect to the Kafka broker.
        """
        try:
            self.consumer = Consumer(kafka_config)
            self.consumer.subscribe([self.topic])
            logging.info(f"Connected to Kafka broker: {kafka_config.get('bootstrap.servers')}")
        except KafkaException as e:
            logging.error(f"Failed to connect to Kafka broker: {e}")
            raise

    def consume(self):
        """
        Consume data from Kafka and write it to a file.
        """
        try:
            os.makedirs(os.path.dirname(self.raw_data_path), exist_ok=True)
            with open(self.raw_data_path, 'a') as file:
                while not self.stop_event.is_set():
                    msg = self.consumer.poll(timeout=1.0)

                    if msg is None or msg.error():
                        if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                            logging.error(f"Kafka error: {msg.error()}")
                        continue

                    try:
                        # Deserialize the Avro message
                        data = self.avro_deserializer(
                            msg.value(),
                            SerializationContext(self.topic, MessageField.VALUE)
                        )

                        if data is None:
                            logging.warning("Received null data after deserialization.")
                            continue

                        logging.info(f"Deserialized data: {data}")

                        self.write_message_to_file(file, data)

                    except Exception as e:
                        logging.error(f"Error deserializing message: {e}")
        except Exception as e:
            logging.error(f"Error in consume: {e}")
        finally:
            self.consumer.close()


    @staticmethod
    def delivery_report(err, msg):
        """
        Logs the delivery report for each consumed message.
        """
        if err is not None:
            logging.error(f"Message failed: {err}")
        else:
            logging.info(f"Message consumed successfully: {msg}")

    def write_message_to_file(self, file, message):
        """
        Writes a decoded message to the specified file.
        """
        try:

            logging.info(f"Received data: {message}")
            file.write(f"{message['date']} {message['time']} {message['epoch']} "
                    f"{message['moteid']} {message['temperature']} "
                    f"{message['humidity']} {message['light']} {message['voltage']}\n")
            file.flush()
            os.fsync(file.fileno())
        except KeyError as e:
            logging.error(f"Missing expected key in message: {e}")
        except Exception as e:
            logging.error(f"Error writing message to file: {e}")


    def compress(self):
        """
        Periodically compress data using MultiSensorDataGrouper.
        """
        while not self.stop_event.is_set():
            self.stop_event.wait(timeout=self.data_compress_interval)
            if self.stop_event.is_set():
                break
            self.compress_and_store()
            self.clear_raw_data()

    def compress_and_store(self):
        """
        Helper function to compress data and write compressed data to files.
        """
        try:
            df = load_data(self.raw_data_path)
            counter_file = os.path.join(self.compressed_data_path, 'compression_counter.txt')
        
            if os.path.exists(counter_file):
                with open(counter_file, 'r') as f:
                    compression_counter = int(f.read().strip())
            else:
                compression_counter = 0


            compression_counter += 1
            

            with open(counter_file, 'w') as f:
                f.write(str(compression_counter))
            for attribute in self.attributes:    
                base_signals, ratio_signals, total_memory_cost = self.grouper.static_group(df, self.base_moteid, attribute)
                logging.info(f'Attribute: {attribute} - Total memory cost after compression: {total_memory_cost} buckets')
                
                self.write_compressed_data(attribute, base_signals, ratio_signals, total_memory_cost,compression_counter)

        except Exception as e:
            logging.error(f"Error in compress_and_store: {e}")
            raise

    def write_compressed_data(self, attribute, base_signals, ratio_signals, total_memory_cost,compression_counter):
        """
        Write the compressed data to files.
        """
        
        try:
            processed_data = {
                'base_signals': [[int(moteid), self.convert_numpy_types(signal_data)] for moteid, signal_data in base_signals],
                'ratio_signals': {str(int(moteid)): self.convert_numpy_types(signal_data) for moteid, signal_data in ratio_signals.items()},
                'total_memory_cost': total_memory_cost,
                'compression_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            os.makedirs(self.compressed_data_path, exist_ok=True)

            # Set the compressed file name based on the attribute and counter
            compressed_file = f'{self.compressed_data_path}/compressed_{attribute}_{compression_counter}.txt'
        
            with open(compressed_file, 'a') as file:
                json.dump(processed_data, file)
            logging.info(f'Compressed data written to {compressed_file}')
        except Exception as e:
            logging.error(f"Error writing compressed data: {e}")
           
    # ############################################################################################### 
    def decompress_and_store(self, compression_counter):
        """
        Decompress each compressed attribute and store the decompressed data
        in separate JSON files for each attribute.
        """
        try:
            os.makedirs(self.decompressed_data_path, exist_ok=True)
            
            for attribute in self.attributes:
                decompressed_file_path = f'{self.decompressed_data_path}/decompressed_{attribute}_{compression_counter}.txt'          
                compressed_file = f'{self.compressed_data_path}/compressed_{attribute}_{compression_counter}.txt'
                
                if not os.path.exists(compressed_file):
                    logging.warning(f"Compressed file for attribute '{attribute}' does not exist.")
                    continue
                
                skipped_moteids = []  # Track moteids with empty ratio buckets

                reconstructed_data = {
                    "reconstructed_base_signal": [],
                    "reconstructed_other_signals": {},
                    'compression_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                try:
                    with open(compressed_file, 'r') as compressed_data_file:
                        for line in compressed_data_file:
                            try:
                                
                                compressed_data = json.loads(line.strip())
                                base_signals = compressed_data.get('base_signals', [])
                                ratio_signals = compressed_data.get('ratio_signals', {})

                               
                                if not base_signals or len(base_signals[0]) < 2:
                                    logging.warning(f"Base signals for attribute '{attribute}' are empty or malformed.")
                                    continue

                                reconstructed_base_signal = self.grouper.reconstruct_signal(base_signals[0][1])
                                reconstructed_data["reconstructed_base_signal"] = reconstructed_base_signal

                                for moteid, ratio_buckets in ratio_signals.items():
                                    if not ratio_buckets:
                                        skipped_moteids.append(moteid)
                                        continue  # Skip processing this moteid

                                    reconstructed_signal = self.grouper.reconstruct_signal(ratio_buckets, reconstructed_base_signal)
                                    reconstructed_data["reconstructed_other_signals"][moteid] = reconstructed_signal

                            except json.JSONDecodeError as e:
                                logging.error(f"JSON decoding error in line: {line}. Error: {e}")
                                continue

                    with open(decompressed_file_path, 'w') as file:
                        json.dump(reconstructed_data, file)
                        
                    if skipped_moteids:
                        logging.warning(f"Skipped {len(skipped_moteids)} moteids with empty ratio buckets in attribute '{attribute}': {set(skipped_moteids)}")

                    logging.info(f"Decompressed data for attribute '{attribute}' written to {decompressed_file_path}.")

                except Exception as e:
                    logging.error(f"Error reading compressed file for attribute '{attribute}': {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Error in decompress_and_store: {e}")
            raise

    def clear_raw_data(self):
        """
        Clear the raw data file.
        """
        os.makedirs(os.path.dirname(self.raw_data_path), exist_ok=True)
        with open(self.raw_data_path, 'w') as file:
            file.write('')
        logging.info("Raw data file cleared.")

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
        return obj

    def run(self):
        """
        Run the server by starting the consumer and compression threads.
        """
        consumer_thread = threading.Thread(target=self.consume)
        consumer_thread.start()

        compression_thread = threading.Thread(target=self.compress)
        compression_thread.start()

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_event.set()
            logging.info("Shutting down...")
        
        consumer_thread.join()
        compression_thread.join()
        logging.info("Server stopped.")

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

    raw_data_path = './src/server_consumer/data/raw_data.txt'
    compressed_data_path = './src/server_consumer/data/compressed'
    decompressed_data_path = './src/server_consumer/data/decompressed'
    
    schema_registry_url = "http://192.168.1.11:18081"

    server = Server(raw_data_path, compressed_data_path, decompressed_data_path,schema_registry_url)
    server.connect(kafka_conf)
    server.run()
    
