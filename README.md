# Flink Kafka Integration Example

This repository contains example code for integrating Apache Flink with Apache Kafka. It includes a Kafka producer that sends JSON messages to a Kafka topic every 3 seconds and a Flink consumer that reads these messages, prints their values along with data types in JSON format, and processes them.

## Prerequisites

- **Apache Kafka**: Make sure you have Apache Kafka and Zookeeper installed and running.
- **Python 3.x**: Ensure Python 3.x is installed on your system.
- **Apache Flink**: Make sure you have Apache Flink installed.
- **Kafka-Python**: Kafka client for Python.

## Installation

### Kafka Setup

1. **Start Zookeeper**:
    ```sh
    ./zookeeper-server-start.sh ../config/zookeeper.properties
    ```

2. **Start Kafka**:
    ```sh
    ./kafka-server-start.sh ../config/server.properties
    ```

3. **Create Kafka Topic**:
    ```sh
    ./kafka-topics.sh --create --topic pwdtopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```

### Python Setup

1. **Create a Virtual Environment** (optional but recommended):
    ```sh
    python -m venv venv
    source venv/bin/activate
    ```

2. **Install Required Python Packages**:
    ```sh
    pip install kafka-python
    ```

### Flink Setup

1. **Download and Extract Apache Flink**:
    ```sh
    wget https://downloads.apache.org/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.11.tgz
    tar -xzf flink-1.17.1-bin-scala_2.11.tgz
    ```

2. **Download Kafka Connector for Flink**:
    ```sh
    wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar -P /Users/tamilselvans/Downloads/
    ```

## Running the Example

### Start the Kafka Producer

1. Save the following code to `kafka_producer.py`:

    ```python
    import json
    import time
    from kafka import KafkaProducer

    def json_serializer(data):
        return json.dumps(data).encode('utf-8')

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    message_template = {
        "name": "Flink doc",
        "size": 22,
        "unit": "bytes",
        "encoded": True,
        "content": "This is a test content"
    }

    if __name__ == "__main__":
        while True:
            producer.send('pwdtopic', message_template)
            print(f"Sent: {message_template}")
            time.sleep(3)
    ```

2. Run the Kafka producer:
    ```sh
    python kafka_producer.py
    ```

### Start the Flink Consumer

1. Save the following code to `flink_consumer.py`:

    ```python
    import logging
    import json
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.common import SimpleStringSchema, WatermarkStrategy
    from pyflink.datastream.functions import MapFunction
    from pyflink.datastream.stream_execution_environment import RuntimeExecutionMode

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)

    class PrintWithTypeMapFunction(MapFunction):
        def map(self, value):
            try:
                data = json.loads(value)
                type_info = {key: type(val).__name__ for key, val in data.items()}
                type_info_json = json.dumps(type_info, indent=2)
                print(type_info_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON: {e}")
            return value

    class SourceData:
        def __init__(self, env):
            self.env = env
            jar_path = "file:///Users/tamilselvans/Downloads/flink-sql-connector-kafka-1.17.1.jar"
            self.env.add_jars(jar_path)
            logger.info(f"Added JAR: {jar_path}")
            self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
            self.env.set_parallelism(1)
            logger.info("Initialized Flink environment with streaming mode and parallelism 1")

        def get_data(self):
            logger.info("Setting up Kafka source")
            source = KafkaSource.builder() \
                .set_bootstrap_servers("localhost:9092") \
                .set_topics("pwdtopic") \
                .set_group_id("my-group") \
                .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .build()
            logger.info("Kafka source set up complete")

            logger.info("Adding Kafka source to the environment")
            stream = self.env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            stream.map(PrintWithTypeMapFunction())
            logger.info("Kafka source added to the environment")

            logger.info("Starting Flink job")
            self.env.execute("source")
            logger.info("Flink job started")

    if __name__ == "__main__":
        try:
            env = StreamExecutionEnvironment.get_execution_environment()
            source_data = SourceData(env)
            source_data.get_data()
            logger.info("Flink job execution complete")
        except Exception as e:
            logger.error(f"Failed to execute Flink job: {e}")
    ```

2. Run the Flink consumer:
    ```sh
    python pyflink_datatype.py
    ```
## input

{    “name”: “Flink doc”,    “size”: 22    “unit”: “bytes”,    “encoded”: true,    “content”: “This is a test content”  }
## Output

The Flink consumer will print the JSON object with the data types of each key in the consumed Kafka messages. The output should look similar to this:

```json
{
  "name": "str",
  "size": "int",
  "unit": "str",
  "encoded": "bool",
  "content": "str"
}
