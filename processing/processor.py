# processing/processor.py
import os
os.environ['_JAVA_OPTIONS'] = '-Duser.timezone=UTC'
import pathlib
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    lib_dir = os.path.abspath("lib")
    jar_urls = [pathlib.Path(os.path.join(lib_dir, jar)).as_uri() for jar in os.listdir(lib_dir) if jar.endswith(".jar")]
    
    t_env.get_config().set("pipeline.jars", ";".join(jar_urls))
    t_env.get_config().set("table.exec.source.idle-timeout", "2000 ms")
    t_env.get_config().set("table.local-time-zone", "UTC")

    print("Flink Environment Booted. Connecting to Kafka...")

    source_ddl = """
        CREATE TABLE raw_telemetry (
            satellite_id STRING,
            event_timestamp BIGINT,
            temperature_c DOUBLE,
            voltage_v DOUBLE,
            cpu_utilization DOUBLE,
            status STRING,
            ts AS TO_TIMESTAMP(FROM_UNIXTIME(event_timestamp / 1000)),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'telemetry.raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_telemetry_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://localhost:28081'
        )
    """
    t_env.execute_sql(source_ddl)

    sink_ddl = """
        CREATE TABLE aggregated_telemetry (
            satellite_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            max_temperature DOUBLE,
            avg_cpu DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/permutex_db',
            'table-name' = 'satellite_telemetry_gold',
            'username' = 'permutex_user',
            'password' = 'permutex_password',
            'sink.buffer-flush.max-rows' = '100',  -- Batch inserts for performance
            'sink.buffer-flush.interval' = '2s'    -- Flush every 2 seconds
        )
    """
    t_env.execute_sql(sink_ddl)


    query = """
        INSERT INTO aggregated_telemetry
        SELECT 
            satellite_id,
            TUMBLE_START(ts, INTERVAL '10' SECOND) as window_start,
            TUMBLE_END(ts, INTERVAL '10' SECOND) as window_end,
            MAX(temperature_c) as max_temperature,
            AVG(cpu_utilization) as avg_cpu
        FROM raw_telemetry
        GROUP BY 
            TUMBLE(ts, INTERVAL '10' SECOND),
            satellite_id
    """
    
    print("Executing Stateful Tumbling Window Aggregation... (Waiting for 10 seconds of data)")
    t_env.execute_sql(query).wait()

if __name__ == '__main__':
    main()