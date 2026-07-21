import requests
import sqlite3
import time
import os
os.makedirs('/data', exist_ok=True)
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
DB_PATH = "/data/transit.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stop_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            arrival_time INTEGER,
            departure_time INTEGER,
            feed_timestamp INTEGER,
            received_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def fetch_feed():
    try:
        response = requests.get(FEED_URL, timeout=10)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def store_updates(feed):
    if feed is None:
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    count = 0
    feed_timestamp = feed.header.timestamp
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            for stu in entity.trip_update.stop_time_update:
                cursor.execute('''
                    INSERT INTO stop_updates (trip_id, route_id, stop_id, arrival_time, departure_time, feed_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    trip.trip_id,
                    trip.route_id,
                    stu.stop_id,
                    stu.arrival.time if stu.HasField('arrival') else None,
                    stu.departure.time if stu.HasField('departure') else None,
                    feed_timestamp,
                ))
                count += 1
    conn.commit()
    conn.close()
    print(f"Stored {count} records")

if __name__ == "__main__":
    init_db()
    while True:
        feed = fetch_feed()
        store_updates(feed)
        time.sleep(30)
