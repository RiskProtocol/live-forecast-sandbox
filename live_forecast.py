import os
import time
import datetime
import websocket
import requests
import rel
import json
import sqlite3
from dotenv import load_dotenv

load_dotenv()

con = sqlite3.connect("test.db")
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS test (time TEXT, asset TEXT,"
            "ReferenceRateUSD TEXT, cm_sequence_id TEXT)")
con.commit()


class LiveForecast:
    def __init__(self):
        self.ws = None
        self.log_path = f"logs/live_forecast_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        self.log_file = open(self.log_path, "a")
        self.coinmetrics_api_key = os.getenv("COINMETRICS_API_KEY")
        self.betterstack_api_key = os.getenv("BETTERSTACK_API_KEY")
        self.betterstack_incident_name = os.getenv("BETTERSTACK_INCIDENT_NAME")
        self.betterstack_incident_requester_email = os.getenv("BETTERSTACK_INCIDENT_REQUESTER_EMAIL")

    def run(self):
        print("Starting live forecast")
        websocket.enableTrace(True)
        self.run_websocket()

    def run_websocket(self):
        try:
            print("Running websocket")
            wss_cm_endpoint = (
                "wss://api.coinmetrics.io/v4/timeseries-stream/asset-metrics"
                "?api_key=" + self.coinmetrics_api_key +
                "&assets=btc&frequency=1s"
                "&metrics=ReferenceRateUSD"
            )
            ws = websocket.WebSocketApp(
                wss_cm_endpoint,
                on_open=self.coinmetrics_on_open,
                on_message=self.coinmetrics_on_message,
                on_error=self.coinmetrics_socket_error,
                on_close=self.coinmetrics_on_close,
            )

            self.ws = ws

            # Rel module is a drop-in replacement for pyevent
            # that completely emulates pyevent's interface,
            # behavior and functionality on any system with Python.
            # It can use pyevent if preferred/available, and otherwise rel
            # will use the fastest method supported by the system
            ws.run_forever(
                dispatcher=rel,
                reconnect=5,
            )
            rel.signal(2, rel.abort)  # If Keyboard Interrupt
            rel.dispatch()
        except Exception as e:
            print(e)

    def coinmetrics_on_message(self, ws, message):
        asset = json.loads(message)
        timestamp = asset["time"]
        self.log_file.write(f"{timestamp} - {asset}\n")
        self.log_file.flush()

        # Simulate a write and fetch from the database
        # insert into test (time, asset, ReferenceRateUSD, cm_sequence_id) values (?, ?, ?, ?)
        cur.execute(
            "INSERT INTO test (time, asset, ReferenceRateUSD, cm_sequence_id) values (?, ?, ?, ?)",
            (timestamp, asset["asset"], asset["ReferenceRateUSD"], asset["cm_sequence_id"]),
        )
        con.commit()

        fetch_gaps_query = """
        WITH numbered_rows AS (
        SELECT
            time,
            LAG(time) OVER (ORDER BY time) AS prev_time,
            ROW_NUMBER() OVER (ORDER BY time) AS row_num
        FROM test
        )
        SELECT
            row_num,
            time,
            prev_time,
            (julianday(time) - julianday(prev_time)) * 86400 AS time_gap_seconds
        FROM numbered_rows
        WHERE (julianday(time) - julianday(prev_time)) * 86400 > 2
        ORDER BY time;
        """

        # After saving to db, query db and check for gaps
        cur.execute(fetch_gaps_query)
        rows = cur.fetchall()
        print(rows)
        if len(rows) > 0 and not hasattr(self, 'incident_sent'):
            self.send_incident(
                {
                    "message": "Gaps in data",
                    "description": f"Gaps in data: {rows}",
                }
            )
            self.incident_sent = True

    def coinmetrics_on_open(self, ws):
        self.log_file.write("Connection is open\n")
        self.log_file.flush()

    def coinmetrics_on_close(self, ws, close_status_code, close_msg):
        self.log_file.write(
            f"Connection is closed with status code : {close_status_code} and message : {close_msg}\n"
        )
        # Check for specific close code and message to reconnect
        if close_status_code == 1001 and "Server is going away" in close_msg:
            self.log_file.write("Reconnection advised by the server.\n")
            self.initiate_reconnection()
        else:
            self.log_file.write("Connection closed without specific close code or message.\n")

        con.close()

    def coinmetrics_socket_error(self, ws, error):
        self.log_file.write(f"Model ended because of error from Coinmetrics: {error}\n")

    def initiate_reconnection(self):
        self.log_file.write("Initiating reconnection\n")
        attempt = 0
        max_attempts = 5
        base_delay = 1
        max_delay = 32

        while attempt < max_attempts:
            self.run_websocket()
            success = self.ws.sock and self.ws.sock.connected
            if success:
                self.log_file.write(f"Connected on attempt {attempt + 1}\n")
                return True

            wait_time = min(base_delay * (2 ** attempt), max_delay)
            self.log_file.write(f"Waiting for {wait_time} seconds before next attempt...\n")
            time.sleep(wait_time)

            attempt += 1

        self.log_file.write(f" Failed to connect after {max_attempts} attempts\n")
        self.log_file.flush()
        return False

    def send_incident(self, logs):
        try:
            request = requests.post("https://uptime.betterstack.com/api/v2/incidents",
                                    headers={"Content-Type": "application/json",
                                             "Authorization": "Bearer " + self.betterstack_api_key},
                                    json={
                                            "name": f"{logs['message']}",
                                            "summary": f"FROM: {self.betterstack_incident_name}",
                                            "requester_email": self.betterstack_incident_requester_email,
                                            "description": f"{logs['description']}",
                                            "email": True
                                        }
                                    )
            print(f"Incident status code: {str(request.status_code)}")
        except Exception as e:
            print(f"Error sending incident: {str(e)}")


if __name__ == "__main__":
    live_forecast = LiveForecast()
    live_forecast.run()  # pragma: no cover
