import pandas as pd
import time
from confluent_kafka import Producer
import json

KAFKA_ENABLED = True

class SensorSimulation():
    def __init__(self, data_file: str, kafka_config: dict, topic: str):
        self.data_file = data_file
        self.kafka_config = kafka_config
        self.topic = topic
        if KAFKA_ENABLED:
            self.producer = Producer(self.kafka_config)

    def delivery_report(self, err, msg):
        """
        Callback function to report message delivery status.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self):
        df = pd.read_csv(self.data_file)
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df = df.dropna(subset=['datetime'])

        try:
            for i in range(len(df)):
                row = df.iloc[i]

                # Prepare the message to send to Kafka
                message = {
                    'datetime': str(row['datetime']),
                    'moteid': int(row['moteid']),
                    'temperature': row['temperature'],
                    'humidity': row['humidity'],
                    'light': row['light'],
                    'voltage': row['voltage']
                }

                if KAFKA_ENABLED:
                    self.producer.produce(
                        self.topic,
                        value=json.dumps(message),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)

                print(f"Produced to Kafka -> Moteid: {row['moteid']}, Time: {row['datetime']}, "
                    f"Temperature: {row['temperature']}, Humidity: {row['humidity']}, "
                    f"Light: {row['light']}, Voltage: {row['voltage']}")

                # For the last row, we don't need to calculate sleep time
                if i < len(df) - 1:
                    time_diff = (df['datetime'].iloc[i + 1] - df['datetime'].iloc[i]).total_seconds()
                    time.sleep(time_diff)

        except KeyboardInterrupt:
            print("\nSimulation stopped by user.")
        
        # Ensure all messages are sent before exiting
        self.producer.flush()
        print(f"Simulation finished for {self.data_file}!")


if __name__ == "__main__":
    # Set the Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:19092',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }

    # Set the Kafka topic name
    topic = "sensor_data"

    # Path to the combined data file
    data_file = "./data/processed/data.csv"

    # Create an instance of the SensorSimulation
    sensor_simulation = SensorSimulation(data_file=data_file, kafka_config=kafka_config, topic=topic)
    sensor_simulation.run()