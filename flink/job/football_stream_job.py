from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from dotenv import load_dotenv
import os

load_dotenv()

kafka_broker = os.getenv("KAFKA_BROKER")
confluent_api_key = os.getenv("CONFLUENT_STAGE_KEY")
confluent_api_secret = os.getenv("CONFLUENT_STAGE_SECRET")
sport_radar_key = os.getenv("SPORTS_RADAR_KEY")

def process_competitions():
    # Set up the execution and table environments
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # Optional checkpoint configurations
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)  # Minimum 5 seconds between checkpoints
    env.get_checkpoint_config().set_checkpoint_timeout(60000)  # Timeout for a checkpoint (60 seconds)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)  # Only one checkpoint at a time


    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Define Kafka source DDL
    kafka_source_ddl = f"""
        CREATE TABLE kafka_source (
            id STRING,
            region STRING,
            name STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'football_global_competitions',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id' = 'flink-source-group',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_api_secret}";',
            'scan.startup.mode' = 'earliest-offset',
            'properties.request.timeout.ms' = '60000',
            'properties.retry.backoff.ms' = '500',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """
    t_env.execute_sql(kafka_source_ddl)

    # Define Kafka sink DDL
    kafka_sink_ddl = f"""
        CREATE TABLE kafka_sink (
            id STRING,
            region STRING,
            name STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'processed_football_global_competitions',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id' = 'flink-sink-group',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_api_secret}";',
            'format' = 'json'
        )
    """
    t_env.execute_sql(kafka_sink_ddl)

    # Transform data and write to sink
    t_env.sql_query("""
        SELECT id, region, name
        FROM kafka_source
    """).execute_insert("kafka_sink")

    print("Debug: Data is being processed and written to the sink.")

if __name__ == '__main__':
    process_competitions()