import json
import sqlite3
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transit-raw',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'
)

conn = sqlite3.connect('transit.db')
cursor = conn.cursor()

print("Listening for messages and writing to transit.db...")
count = 0
for message in consumer:
    record = message.value
    cursor.execute('''
        INSERT INTO stop_updates (trip_id, route_id, stop_id, arrival_time, departure_time, feed_timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        record.get('trip_id'),
        record.get('route_id'),
        record.get('stop_id'),
        record.get('arrival_time'),
        record.get('departure_time'),
        record.get('feed_timestamp'),
    ))
    conn.commit()
    count += 1
    if count % 50 == 0:
        print(f"Written {count} records so far")