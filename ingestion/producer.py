# ingestion/producer.py
import asyncio
import random
import time
from datetime import datetime
from pydantic import BaseModel, Field, ValidationError
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:28081"
TOPIC_NAME = "telemetry.raw"

class TelemetryPayload(BaseModel):
    satellite_id: str
    event_timestamp: int
    temperature_c: float = Field(..., ge=-100.0, le=200.0) # Strictly bounded
    voltage_v: float = Field(..., ge=0.0, le=50.0)
    cpu_utilization: float = Field(..., ge=0.0, le=100.0)
    status: str

with open("schemas/telemetry.avsc", "r") as f:
    schema_str = f.read()

def delivery_report(err, msg):
    """Callback for Kafka to confirm message delivery."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


async def simulate_satellite(satellite_id: str, producer: SerializingProducer):
    """Asynchronous loop simulating continuous data stream from a single satellite."""
    print(f"[{satellite_id}] Simulator booted and streaming...")
    
    while True:
        try:
            raw_data = {
                "satellite_id": satellite_id,
                "event_timestamp": int(time.time() * 1000),
                "temperature_c": round(random.uniform(-50.0, 150.0), 2),
                "voltage_v": round(random.uniform(10.0, 48.0), 2),
                "cpu_utilization": round(random.uniform(10.0, 99.0), 2),
                "status": random.choices(["NOMINAL", "DEGRADED", "CRITICAL"], weights=[0.85, 0.10, 0.05])[0]
            }

            validated_data = TelemetryPayload(**raw_data).model_dump()

            producer.produce(
                topic=TOPIC_NAME,
                key=satellite_id, 
                value=validated_data,
                on_delivery=delivery_report
            )
            producer.poll(0) 

        except ValidationError as e:
            print(f"[{satellite_id}] Data Corruption Caught by Pydantic: {e}")
        except Exception as e:
            print(f"[{satellite_id}] System Error: {e}")

        await asyncio.sleep(random.uniform(0.1, 0.5))

async def main():
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda dict_obj, ctx: dict_obj 
    )

    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'linger.ms': 10, 
        'compression.type': 'snappy' 
    }
    
    producer = SerializingProducer(producer_conf)

   
    satellites = ["SAT-ALPHA-01", "SAT-BETA-02", "SAT-GAMMA-03", "SAT-DELTA-04", "SAT-EPSILON-05"]
    tasks = [simulate_satellite(sat, producer) for sat in satellites]
    
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    try:
        print("Starting PermuteX Telemetry Grid INGESTION...")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPipeline Terminated safely.")