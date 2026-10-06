import csv
import json
import time
import os
from confluent_kafka import Producer

# Updated path to match your current file location
csv_file = r"C:/Users/KavyaAgrawal/Downloads/Data-20260414T080156Z-3-001/Data/SPTStore Data Files/Transactions/MavenMarket_Transactions_1998.csv"

# Check if file exists
if not os.path.exists(csv_file):
    print(f"ERROR: Cannot find {csv_file}")
    exit(1)

# Confluent Cloud Kafka Credentials
# Replace these with your newly rotated credentials
conf = {
    'bootstrap.servers': 'pkc-w77k7w.centralus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OPY6NFHYT2UWERIN',
    'sasl.password': 'cfltjtlMTVzl047tjHEdlt3I+mO091xgomV2eK0IrQAj/tW07B4ZMxDNR+nY93ww'
}

producer = Producer(conf)
topic = 'orders-stream'


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


print("Starting real-time streaming producer for MavenMarket_Transactions_1998.csv...")

with open(csv_file, mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    count = 0

    for row in reader:
        payload_data = {
            "transaction_date": row["transaction_date"],
            "stock_date": row["stock_date"],
            "product_id": int(row["product_id"]),
            "customer_id": int(row["customer_id"]),
            "store_id": int(row["store_id"]),
            "quantity": int(row["quantity"])
        }

        producer.produce(
            topic=topic,
            key=str(row["customer_id"]),
            value=json.dumps(payload_data),
            callback=delivery_report
        )

        producer.poll(0)

        count += 1

        if count % 50 == 0:
            print(f"Streamed {count} live order events into Kafka...")

        # Sends approximately 10 records per second
        time.sleep(0.1)

producer.flush()

print(f"Completed streaming. Total records sent: {count}")