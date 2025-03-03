from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os

def kafka_uppercase_job():
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
    
    # Define source table
    source_ddl = """
    CREATE TABLE source_table (
        input_string STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'input-topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'test-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
    
    # Define sink table
    sink_ddl = """
    CREATE TABLE sink_table (
        output_string STRING
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
    
    # Transform data and insert into sink table
    t_env.execute_sql("""
    INSERT INTO sink_table
    SELECT UPPER(input_string) AS output_string FROM source_table
    """)

if __name__ == '__main__':
    kafka_uppercase_job()


# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# import os

# def kafka_to_kafka_job():
#     # Create execution environment
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
    
#     # Create table environment
#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, settings)
    
#     t_env.get_config().get_configuration().set_string("kafka.request.timeout.ms", "60000")
#     t_env.get_config().get_configuration().set_string("kafka.max.block.ms", "120000")


#     # Add required JARs (ensure correct version is used)
#     kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 
#                              '../jars/flink-sql-connector-kafka-3.4.0-1.20.jar')
#     t_env.get_config().get_configuration().set_string(
#         "pipeline.jars", f"file://{kafka_jar}")
    
#     # Define source table
#     source_ddl = """
#     CREATE TABLE source_table (
#         input_string STRING
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'input-topic',
#         'properties.bootstrap.servers' = 'kafka:9092',
#         'properties.group.id' = 'test-group',
#         'scan.startup.mode' = 'latest-offset',
#         'format' = 'json'
#     )
#     """
    
#     # Define sink table
#     sink_ddl = """
#     CREATE TABLE sink_table (
#         output_string STRING
#     ) WITH (
#         'connector' = 'kafka',
#         'topic' = 'output-topic',
#         'properties.bootstrap.servers' = 'kafka:9092',
#         'format' = 'json'
#     )
#     """
    
#     # Register tables
#     t_env.execute_sql(source_ddl)
#     t_env.execute_sql(sink_ddl)
    
#     # Transform data and insert into sink table
#     t_env.execute_sql("""
#     INSERT INTO sink_table
#     SELECT UPPER(input_string) AS output_string FROM source_table
#     """)

# if __name__ == '__main__':
#     kafka_to_kafka_job()