# pyflink_job/kafka_processing.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os

def kafka_to_kafka_job():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # Add required JARs
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 
                           '../jars/flink-sql-connector-kafka-1.17.1.jar')
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file://{kafka_jar}")
    
    # Define source
    source_ddl = """
    CREATE TABLE source_table (
        user_id STRING,
        item_id STRING,
        category_id STRING,
        behavior STRING,
        ts TIMESTAMP(3),
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'input-topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'test-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    
    # Define sink
    sink_ddl = """
    CREATE TABLE sink_table (
        user_id STRING,
        item_id STRING,
        category_id STRING,
        behavior STRING,
        ts TIMESTAMP(3),
        processing_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'output-topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
    )
    """
    
    # Register tables
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    
    # Execute query
    t_env.execute_sql("""
    INSERT INTO sink_table
    SELECT
        user_id,
        item_id,
        category_id,
        behavior,
        ts,
        proctime as processing_time
    FROM source_table
    """)

if __name__ == '__main__':
    kafka_to_kafka_job()