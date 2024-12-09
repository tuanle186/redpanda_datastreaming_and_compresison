import pandas as pd
import time
from confluent_kafka import Producer, KafkaException
import json
import logging
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Client:
    def __init__(self, dataset_path: str):
        self.topic = 'sensor-data'
        self.producer: Optional[Producer] = None
        self.data = self.load_data(dataset_path)
        self.retry_delay = 5  

    @staticmethod
    def load_data(data_file: str) -> pd.DataFrame:
        """
        Load the sensor data from the CSV file.
        """
        try:
            df = pd.read_csv(data_file)
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df.dropna(subset=['datetime'], inplace=True)
            logging.info("Data loaded successfully.")
            return df
        except Exception as e:
            logging.error(f"Error loading data file: {e}")
            raise

    def connect(self, kafka_config: Dict[str, str], retries: int = 3):
        """
        Connect to the Redpanda broker with retry logic.
        """
        attempt = 0
        while attempt < retries:
            try:
                self.producer = Producer(kafka_config)
                logging.info(f"Connected to Redpanda broker: {kafka_config['bootstrap.servers']}")
                return
            except KafkaException as e:
                attempt += 1
                logging.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < retries:
                    time.sleep(self.retry_delay)
                else:
                    logging.error("Max retries reached. Could not connect to the broker.")
                    raise

    def produce(self, message: dict, retries: int = 3):
        """
        Produce a message to the Kafka topic with retry logic and detailed logging of outgoing data.
        """
        attempt = 0
        while attempt < retries:
            try:
                # Log the message content in JSON format
                logging.info(f"Producing message: {json.dumps(message)}")
                
                self.producer.produce(self.topic, value=json.dumps(message), callback=self.delivery_report)
                self.producer.poll(0)
                return
            except KafkaException as e:
                attempt += 1
                logging.warning(f"Attempt {attempt} failed to produce message: {e}")
                if attempt < retries:
                    time.sleep(self.retry_delay)
                else:
                    logging.error("Max retries reached. Message failed to send.")


    @staticmethod
    def delivery_report(err, msg):
        """
        Callback function to report message delivery status.
        """
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def prepare_message(self, row: pd.Series) -> dict:
        """
        Prepare the message to be sent to Kafka from a DataFrame row.
        """
        return {
            'date': str(row['date']),
            'time': str(row['time']),
            'epoch': int(row['epoch']),
            'moteid': int(row['moteid']),
            'temperature': row['temperature'],
            'humidity': row['humidity'],
            'light': row['light'],
            'voltage': row['voltage']
        }

    def run(self):
        """
        Run the simulation to produce messages to Kafka.
        """
        if not self.producer:
            logging.error("Kafka producer not connected.")
            return

        try:
            for i in range(len(self.data)):
                row = self.data.iloc[i]
                message = self.prepare_message(row)


                while True:
                    try:
                        self.produce(message)
                        break
                    except KafkaException as e:
                        logging.error(f"Error producing message: {e}. Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)


                if i < len(self.data) - 1:
                    time_diff = (self.data['datetime'].iloc[i + 1] - self.data['datetime'].iloc[i]).total_seconds()
                    if time_diff > 0:
                        time.sleep(time_diff)

        except KeyboardInterrupt:
            logging.info("Simulation stopped by user.")
        
        finally:
            if self.producer:
                self.producer.flush()
            logging.info("Simulation finished!")


if __name__ == "__main__":
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
        client.connect(kafka_conf, retries=5)  # Increased retries for connection
        client.run()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
