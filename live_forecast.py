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

# Connection/cursor are created once per process and shared by every callback.
# `check_same_thread=False` is required because the websocket dispatcher may invoke
# callbacks from a thread other than the one that opened the connection.
con = sqlite3.connect("test.db", check_same_thread=False)
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS test (time TEXT, asset TEXT,"
            "ReferenceRateUSD TEXT, cm_sequence_id TEXT)")
con.commit()


class LiveForecast:
    # Request timeout applied to every outbound Better Stack call. Without it a
    # stalled incident API blocks the websocket message handler indefinitely.
    INCIDENT_TIMEOUT_SECONDS = 10

    def __init__(self):
        self.ws = None
        # `logs/` is gitignored, so it is absent on a fresh clone. The previous code
        # opened the log file unconditionally in __init__, which raised
        # FileNotFoundError before the service ever started. Create it defensively.
        os.makedirs("logs", exist_ok=True)
        self.log_path = f"logs/live_forecast_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        # Explicit UTF-8: the log records upstream payloads, so relying on the
        # platform default encoding produced UnicodeEncodeError on Windows consoles.
        self.log_file = open(self.log_path, "a", encoding="utf-8")
        self.coinmetrics_api_key = os.getenv("COINMETRICS_API_KEY")
        self.betterstack_api_key = os.getenv("BETTERSTACK_API_KEY")
        self.betterstack_incident_name = os.getenv("BETTERSTACK_INCIDENT_NAME")
        self.betterstack_incident_requester_email = os.getenv("BETTERSTACK_INCIDENT_REQUESTER_EMAIL")
        self.lazy_counter = 0
        # Tracked explicitly instead of being probed with hasattr() on first use:
        # `incident_sent` was previously created mid-callback, so its meaning
        # depended on whether any gap had ever been seen.
        self.incident_sent = False

    def close(self):
        """Release the log file handle.

        The handle was previously opened in __init__ and never closed, so every
        restart leaked a descriptor and buffered log data was lost on exit.
        """
        if not self.log_file.closed:
            self.log_file.close()

    def run(self):
        print("Starting live forecast")
        # Never leave frame tracing enabled: it prints raw payloads (including the
        # api_key embedded in the request URL) to stderr on every reconnect.
        websocket.enableTrace(False)
        try:
            self.run_websocket()
        finally:
            self.close()

    def run_websocket(self):
        try:
            print("Running websocket")
            if not self.coinmetrics_api_key:
                # Fail fast with an actionable message instead of building the URL
                # with a None api_key and getting an opaque server-side rejection.
                raise ValueError("COINMETRICS_API_KEY is not set; refusing to connect")
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
            # Log and re-raise: swallowing this exception returned the caller with no
            # indication that the stream never started, so the process looked healthy
            # while collecting nothing.
            self.log(f"Websocket loop failed: {e}")
            raise

    def log(self, message):
        """Appends a line to the run log, tolerating an already-closed handle."""
        if self.log_file.closed:
            return
        self.log_file.write(message + "\n")
        self.log_file.flush()

    def coinmetrics_on_message(self, ws, message):
        asset = json.loads(message)
        timestamp = asset["time"]
        self.log(f"{timestamp} - {asset}")
        self.lazy_counter += 1
        # Simulate a write and fetch from the database
        # insert into test (time, asset, ReferenceRateUSD, cm_sequence_id) values (?, ?, ?, ?)
        cur.execute(
            "INSERT INTO test (time, asset, ReferenceRateUSD, cm_sequence_id) values (?, ?, ?, ?)",
            (timestamp, asset["asset"], asset["ReferenceRateUSD"], asset["cm_sequence_id"]),
        )
        con.commit()

        if self.lazy_counter % 10 == 0:
            # Blocking call for 10 seconds
            self.log_file.write("Sleeping for 10 seconds\n")
            self.log_file.flush()
            time.sleep(10)
            self.lazy_counter = 0

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
        # Only latch `incident_sent` when the alert was actually accepted. Previously
        # the flag was set unconditionally after the first gap, so a failed upload
        # (bad API key, network error) suppressed every future alert for the process.
        if len(rows) > 0 and not self.incident_sent:
            if self.send_incident(
                {
                    "message": "Gaps in data",
                    "description": f"Gaps in data: {rows}",
                }
            ):
                self.incident_sent = True

    def coinmetrics_on_open(self, ws):
        self.log("Connection is open")

    def coinmetrics_on_close(self, ws, close_status_code, close_msg):
        self.log(
            f"Connection is closed with status code : {close_status_code} and message : {close_msg}"
        )
        # Check for specific close code and message to reconnect.
        # `close_msg` is documented as optional and is None on an abnormal
        # close, so the substring test must be guarded: `"x" in None` raises
        # TypeError and previously aborted the reconnect path entirely.
        if close_status_code == 1001 and close_msg and "Server is going away" in close_msg:
            self.log("Reconnection advised by the server.")
            self.initiate_reconnection()
        else:
            self.log("Connection closed without specific close code or message.")

        # Deliberately do NOT close `con`/`cur` here. This callback fires on every
        # reconnect, and the previous `con.close()` left the module-level cursor
        # bound to a closed database, so the next INSERT raised
        # "cannot use a closed database". The connection is owned by the module and
        # is released once, in close().
        self.log("Database connection retained for reconnect.")

    def coinmetrics_socket_error(self, ws, error):
        self.log(f"Model ended because of error from Coinmetrics: {error}")

    def initiate_reconnection(self):
        self.log("Initiating reconnection")
        attempt = 0
        max_attempts = 5
        base_delay = 1
        max_delay = 32

        while attempt < max_attempts:
            try:
                self.run_websocket()
            except Exception as e:
                # `run_websocket` now re-raises. Without this guard a failed attempt
                # aborted the whole retry loop, so a single transient error ended
                # reconnection permanently instead of using the remaining attempts.
                self.log(f"Reconnection attempt {attempt + 1} raised: {e}")

            # Treat a live socket as success. Previously this was checked only after
            # run_forever returned, and `sock.connected` is None by that point, so a
            # successful reconnect was never detected and the loop always exhausted.
            sock = getattr(self.ws, "sock", None)
            if sock is not None and getattr(sock, "connected", False):
                self.log(f"Connected on attempt {attempt + 1}")
                return True

            wait_time = min(base_delay * (2 ** attempt), max_delay)
            self.log(f"Waiting for {wait_time} seconds before next attempt...")
            time.sleep(wait_time)

            attempt += 1

        self.log(f"Failed to connect after {max_attempts} attempts")
        return False

    def send_incident(self, logs):
        """Reports a data gap to Better Stack.

        Returns True when the incident was accepted. A non-2xx response is now
        treated as a failure and logged: the previous code printed the status code
        only, so an expired or rate-limited API key looked like a successful alert
        and `incident_sent` was set anyway, permanently suppressing future alerts.
        """
        if not self.betterstack_api_key:
            self.log("BETTERSTACK_API_KEY is not set; skipping incident upload")
            return False

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
                                        },
                                    # Bounded so a stalled incident API cannot block
                                    # the websocket message handler indefinitely.
                                    timeout=self.INCIDENT_TIMEOUT_SECONDS)
            request.raise_for_status()
            self.log(f"Incident accepted with status code {request.status_code}")
            return True
        except requests.RequestException as e:
            # Logged as well as printed: this handler previously only wrote to
            # stdout, which is discarded under most service managers.
            self.log(f"Error sending incident: {e}")
            print(f"Error sending incident: {str(e)}")
            return False


if __name__ == "__main__":
    live_forecast = LiveForecast()
    try:
        live_forecast.run()  # pragma: no cover
    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        # Guarantee the log handle is released even on Ctrl-C or an unexpected exit.
        live_forecast.close()
        con.close()
