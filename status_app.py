from flask import Flask, jsonify
import sqlite3
from datetime import datetime

app = Flask(__name__)

def get_stats():
    conn = sqlite3.connect('transit.db')
    total = conn.execute('SELECT COUNT(*) FROM stop_updates').fetchone()[0]
    routes = conn.execute('SELECT DISTINCT route_id FROM stop_updates').fetchall()
    last_record = conn.execute('SELECT received_at FROM stop_updates ORDER BY id DESC LIMIT 1').fetchone()
    conn.close()
    return {
        "status": "live",
        "total_records": total,
        "distinct_routes": [r[0] for r in routes],
        "last_update": last_record[0] if last_record else None,
        "checked_at": datetime.utcnow().isoformat()
    }

@app.route('/')
def home():
    return '''
    <html>
    <head><title>NJ/NYC Transit Pipeline - Live Status</title></head>
    <body style="font-family: monospace; background: #111; color: #0f0; padding: 40px;">
        <h1>Live Transit Data Pipeline</h1>
        <p>Real-time MTA subway data: Kafka streaming to SQLite</p>
        <p><a href="/status" style="color: #0ff;">View live stats (JSON)</a></p>
    </body>
    </html>
    '''

@app.route('/status')
def status():
    return jsonify(get_stats())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)