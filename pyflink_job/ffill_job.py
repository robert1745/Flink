import os
import json
import pandas as pd
import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf

GARBAGE_VALUE = -2147483650

# Define the forward-fill function using Pandas
def pandas_ffill(data):
    """Apply forward-fill (ffill) on a batch of rows using Pandas."""
    df = pd.DataFrame(data)
    df = df.replace(GARBAGE_VALUE, np.nan).ffill(axis=0)
    return df.to_dict(orient='records')

@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("BF_CellVoltage1", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage10", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage11", DataTypes.FLOAT()),  
    DataTypes.FIELD("BF_CellVoltage12", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage2", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage3", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage4", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage5", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage6", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage7", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage8", DataTypes.FLOAT()),
    DataTypes.FIELD("BF_CellVoltage9", DataTypes.FLOAT()),
    DataTypes.FIELD("event_time", DataTypes.BIGINT()),
    DataTypes.FIELD("vin", DataTypes.STRING())
]))
def forward_fill_udf(*row):
    print(f"inside forward_fill_udf")
    print(f"Input row: {row}")
    column_names = [
        "BF_CellVoltage1", "BF_CellVoltage10", "BF_CellVoltage11",
        "BF_CellVoltage12", "BF_CellVoltage2", "BF_CellVoltage3",
        "BF_CellVoltage4", "BF_CellVoltage5", "BF_CellVoltage6",
        "BF_CellVoltage7", "BF_CellVoltage8", "BF_CellVoltage9",
        "event_time", "vin"
    ]
    
    row_dict = {col: val for col, val in zip(column_names, row)}
    result = pandas_ffill([row_dict])[0]
    
    print(f"Output row: {result}")
    return tuple(result[col] for col in column_names)

def kafka_to_kafka_ffill_job():
    # Create execution environment
    print(f"inside kafka_to_kafka_ffill_job")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    

    t_env.get_config().get_configuration().set_string("kafka.request.timeout.ms", "60000")
    t_env.get_config().get_configuration().set_string("kafka.max.block.ms", "120000")

    # Add required JARs (ensure correct version is used)
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 
                             '../jars/flink-sql-connector-kafka-3.4.0-1.20.jar')
                             
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file://{kafka_jar}")

    print(f"configration jars")
    # Define source table (Kafka input-topic)
    source_ddl = """
        CREATE TABLE source_table (
            BF_CellVoltage1 FLOAT,
            BF_CellVoltage10 FLOAT,
            BF_CellVoltage11 FLOAT,
            BF_CellVoltage12 FLOAT,
            BF_CellVoltage2 FLOAT,
            BF_CellVoltage3 FLOAT,
            BF_CellVoltage4 FLOAT,
            BF_CellVoltage5 FLOAT,
            BF_CellVoltage6 FLOAT,
            BF_CellVoltage7 FLOAT,
            BF_CellVoltage8 FLOAT,
            BF_CellVoltage9 FLOAT,
            event_time BIGINT,
            vin STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'input-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'test-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    print(f"naveen-1")
    # Define sink table (Kafka output-topic)
    sink_ddl = """
        CREATE TABLE sink_table (
            BF_CellVoltage1 FLOAT,
            BF_CellVoltage10 FLOAT,
            BF_CellVoltage11 FLOAT,
            BF_CellVoltage12 FLOAT,
            BF_CellVoltage2 FLOAT,
            BF_CellVoltage3 FLOAT,
            BF_CellVoltage4 FLOAT,
            BF_CellVoltage5 FLOAT,
            BF_CellVoltage6 FLOAT,
            BF_CellVoltage7 FLOAT,
            BF_CellVoltage8 FLOAT,
            BF_CellVoltage9 FLOAT,
            event_time BIGINT,
            vin STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    print(f"naveen-2")

    # Register source and sink tables
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    print(f"naveen-3 source and sink tables registered")
    # Register the forward-fill UDF
    t_env.create_temporary_function("forward_fill_udf", forward_fill_udf)
    print(f"naveen-4 registered the forward-fill UDF")
    # Transform data using the UDF and insert into the sink table
    t_env.execute_sql("""
    INSERT INTO sink_table
    SELECT 
        EXPR$0.BF_CellVoltage1 AS BF_CellVoltage1,
        EXPR$0.BF_CellVoltage10 AS BF_CellVoltage10,
        EXPR$0.BF_CellVoltage11 AS BF_CellVoltage11,
        EXPR$0.BF_CellVoltage12 AS BF_CellVoltage12,
        EXPR$0.BF_CellVoltage2 AS BF_CellVoltage2,
        EXPR$0.BF_CellVoltage3 AS BF_CellVoltage3,
        EXPR$0.BF_CellVoltage4 AS BF_CellVoltage4,
        EXPR$0.BF_CellVoltage5 AS BF_CellVoltage5,
        EXPR$0.BF_CellVoltage6 AS BF_CellVoltage6,
        EXPR$0.BF_CellVoltage7 AS BF_CellVoltage7,
        EXPR$0.BF_CellVoltage8 AS BF_CellVoltage8,
        EXPR$0.BF_CellVoltage9 AS BF_CellVoltage9,
        EXPR$0.event_time AS event_time,
        EXPR$0.vin AS vin
    FROM (
        SELECT forward_fill_udf(
            BF_CellVoltage1, 
            BF_CellVoltage10, 
            BF_CellVoltage11, 
            BF_CellVoltage12, 
            BF_CellVoltage2, 
            BF_CellVoltage3, 
            BF_CellVoltage4, 
            BF_CellVoltage5, 
            BF_CellVoltage6, 
            BF_CellVoltage7, 
            BF_CellVoltage8, 
            BF_CellVoltage9, 
            event_time, 
            vin
        ) AS EXPR$0
        FROM source_table
            )
        """)
    print(f"Swastik-5 inserted into sink table")


if __name__ == '__main__':
    kafka_to_kafka_ffill_job()
