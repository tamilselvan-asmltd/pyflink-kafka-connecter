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
