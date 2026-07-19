import sqlite3

conn = sqlite3.connect('transit.db')
cursor = conn.cursor()

cursor.execute('''
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
print("Database and table created successfully")