import sqlite3

conn = sqlite3.connect('transit.db')

count = conn.execute('SELECT COUNT(*) FROM stop_updates').fetchone()
print("Total records:", count[0])

routes = conn.execute('SELECT DISTINCT route_id FROM stop_updates').fetchall()
print("Distinct routes:", routes)

print("\nLatest 5 records:")
for row in conn.execute('SELECT trip_id, route_id, stop_id, arrival_time, departure_time FROM stop_updates ORDER BY id DESC LIMIT 5'):
    print(row)

conn.close()