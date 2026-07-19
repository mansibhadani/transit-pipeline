import requests
import json
import time
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

FEED_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"  # keep your existing real URL, don't retype it

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_feed():
    try:
        response = requests.get(FEED_URL, timeout=10)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except requests.exceptions.Timeout:
        print("ERROR: request took too long, feed might be down")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: couldn't reach feed - {e}")
        return None
    except Exception as e:
        print(f"ERROR: couldn't decode feed - {e}")
        return None

def send_updates(feed):
    if feed is None:
        return
    count = 0
    feed_timestamp = feed.header.timestamp
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            for stu in entity.trip_update.stop_time_update:
                record = {
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "stop_id": stu.stop_id,
                    "arrival_time": stu.arrival.time if stu.HasField('arrival') else None,
                    "departure_time": stu.departure.time if stu.HasField('departure') else None,
                    "feed_timestamp": feed_timestamp,
                }
                producer.send('transit-raw', value=record)
                count += 1
    producer.flush()
    print(f"Sent {count} records")

if __name__ == "__main__":
    while True:
        feed = fetch_feed()
        send_updates(feed)
        time.sleep(30)