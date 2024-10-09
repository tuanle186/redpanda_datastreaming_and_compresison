from client_producer.client import SensorSimulation

CLIENT_ENABLED = True
SERVER_ENABLED = False


if CLIENT_ENABLED:
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


if SERVER_ENABLED:
    pass
