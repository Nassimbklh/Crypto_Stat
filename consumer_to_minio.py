import json, io, pandas as pd
from confluent_kafka import Consumer
import boto3
from datetime import datetime

c = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "crypto-consumer",
    "auto.offset.reset": "earliest"
})
c.subscribe(["crypto_ticks"])

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

bucket = "crypto-bucket"
try: s3.create_bucket(Bucket=bucket)
except: pass

batch = []
while True:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    batch.append(json.loads(msg.value().decode()))
    if len(batch) >= 200:
        df = pd.DataFrame(batch)
        key = f"ticks_{datetime.utcnow():%Y%m%d_%H%M%S}.parquet"
        buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buf)
        print("OK Uploaded:", key)
        batch = []