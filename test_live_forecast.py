"""Behavioural tests for the live-forecast-sandbox fixes.

Each test reproduces a defect that existed before the remediation and asserts the
corrected behaviour. Run with: python -m pytest test_live_forecast.py -v
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ.setdefault("COINMETRICS_API_KEY", "test-key")
os.environ.setdefault("BETTERSTACK_API_KEY", "test-key")

import live_forecast as lf  # noqa: E402


@pytest.fixture()
def forecast(tmp_path, monkeypatch):
    """Creates a LiveForecast instance rooted in an isolated temp directory."""
    monkeypatch.chdir(tmp_path)
    return lf.LiveForecast()


def _insert(time_value, seq):
    """Writes one row straight into the shared table, bypassing the callback."""
    lf.cur.execute(
        "INSERT INTO test (time, asset, ReferenceRateUSD, cm_sequence_id) "
        "values (?, ?, ?, ?)",
        (time_value, "btc", "1.0", seq),
    )
    lf.con.commit()


def _message():
    return json.dumps({
        "time": "2024-01-01T00:00:00Z",
        "asset": "btc",
        "ReferenceRateUSD": "42000.00",
        "cm_sequence_id": "1234",
    })


class TestLogLifecycle:
    def test_logs_dir_created_on_fresh_clone(self, forecast):
        """`logs/` is gitignored and absent on a clean checkout.

        The constructor previously opened logs/<file> unconditionally, raising
        FileNotFoundError before the service could start.
        """
        assert os.path.isdir("logs")
        assert os.path.exists(forecast.log_path)

    def test_log_file_is_closed_by_close(self, forecast):
        """The log handle was previously opened and never released."""
        assert not forecast.log_file.closed
        forecast.close()
        assert forecast.log_file.closed

    def test_close_is_idempotent(self, forecast):
        """Calling close twice must not raise."""
        forecast.close()
        forecast.close()
        assert forecast.log_file.closed

    def test_log_after_close_does_not_raise(self, forecast):
        """A write after close must degrade quietly, not crash the callback."""
        forecast.close()
        forecast.log("late message")  # must not raise


class TestCloseCallback:
    def test_none_close_msg_does_not_raise(self, forecast, monkeypatch):
        """`"x" in None` raises TypeError.

        websocket-client passes close_msg=None on an abnormal close, which
        previously aborted the reconnect path before it could run.
        """
        monkeypatch.setattr(forecast, "initiate_reconnection", lambda: None)
        forecast.coinmetrics_on_close(None, 1001, None)  # must not raise

    def test_server_going_away_triggers_reconnect(self, forecast, monkeypatch):
        """The documented 1001 + 'Server is going away' case must reconnect."""
        called = {"n": 0}
        monkeypatch.setattr(forecast, "initiate_reconnection", lambda: called.__setitem__("n", called["n"] + 1))
        forecast.coinmetrics_on_close(None, 1001, "Server is going away")
        assert called["n"] == 1

    def test_connection_retained_across_reconnects(self, forecast):
        """con.close() in the callback left the cursor bound to a dead database.

        The fix keeps the connection open, so an INSERT after a close callback
        must still succeed.
        """
        forecast.coinmetrics_on_close(None, 1000, "normal closure")
        # Would raise sqlite3.ProgrammingError against a closed connection.
        _insert("2024-01-01T00:00:00Z", "1")


class TestMessageHandling:
    def test_insert_succeeds_after_repeated_messages(self, forecast):
        """Basic happy path for the persistence callback."""
        forecast.coinmetrics_on_message(None, _message())
        assert lf.cur.execute("SELECT COUNT(*) FROM test").fetchone()[0] >= 1

    def test_incident_not_latched_when_upload_fails(self, forecast, monkeypatch):
        """A failed alert must not permanently suppress future alerts.

        The previous code set incident_sent unconditionally after the first gap,
        so one failed upload silenced the process for its lifetime.
        """
        monkeypatch.setattr(forecast, "send_incident", lambda logs: False)
        # Seed a >2s gap so the detector reports one.
        _insert("2024-01-01T00:00:00Z", "1")
        _insert("2024-01-01T00:00:30Z", "2")

        forecast.coinmetrics_on_message(None, _message())
        assert forecast.incident_sent is False, (
            "incident_sent must stay False when the upload did not succeed"
        )

    def test_incident_latched_when_upload_succeeds(self, forecast, monkeypatch):
        """A successful alert latches the flag so it is sent only once."""
        monkeypatch.setattr(forecast, "send_incident", lambda logs: True)
        _insert("2024-01-01T00:00:00Z", "1")
        _insert("2024-01-01T00:00:30Z", "2")

        forecast.coinmetrics_on_message(None, _message())
        assert forecast.incident_sent is True


class TestIncidentDelivery:
    def test_missing_api_key_skips_upload(self, forecast, monkeypatch):
        """No API key must short-circuit rather than send 'Bearer None'."""
        monkeypatch.setattr(forecast, "betterstack_api_key", None)

        def explode(*args, **kwargs):
            raise AssertionError("HTTP request must not be attempted without a key")

        monkeypatch.setattr(lf.requests, "post", explode)
        assert forecast.send_incident({"message": "m", "description": "d"}) is False

    def test_non_2xx_response_returns_false(self, forecast, monkeypatch):
        """A rejected incident must be reported as a failure.

        The previous code printed the status code and returned None regardless,
        so an expired key looked like a delivered alert.
        """
        class FakeResponse:
            status_code = 401

            def raise_for_status(self):
                import requests as r
                raise r.HTTPError("401 Client Error")

        monkeypatch.setattr(lf.requests, "post", lambda *a, **k: FakeResponse())
        assert forecast.send_incident({"message": "m", "description": "d"}) is False

    def test_success_returns_true(self, forecast, monkeypatch):
        """A 2xx response is reported as success."""
        class FakeResponse:
            status_code = 201

            def raise_for_status(self):
                return None

        monkeypatch.setattr(lf.requests, "post", lambda *a, **k: FakeResponse())
        assert forecast.send_incident({"message": "m", "description": "d"}) is True

    def test_timeout_is_always_supplied(self, forecast, monkeypatch):
        """Every incident POST must be time-bounded."""
        captured = {}

        def fake_post(url, **kwargs):
            captured.update(kwargs)

            class FakeResponse:
                status_code = 201

                def raise_for_status(self):
                    return None

            return FakeResponse()

        monkeypatch.setattr(lf.requests, "post", fake_post)
        forecast.send_incident({"message": "m", "description": "d"})
        assert captured["timeout"] == forecast.INCIDENT_TIMEOUT_SECONDS


class TestStartupGuards:
    def test_missing_coinmetrics_key_fails_fast(self, forecast, monkeypatch):
        """A missing API key must raise immediately, not build '...api_key=None'."""
        monkeypatch.setattr(forecast, "coinmetrics_api_key", None)
        with pytest.raises(ValueError, match="COINMETRICS_API_KEY"):
            forecast.run_websocket()

    def test_trace_is_disabled(self, forecast, monkeypatch):
        """Frame tracing must never be left on; it prints the URL with api_key."""
        enabled = []
        monkeypatch.setattr(lf.websocket, "enableTrace", lambda v: enabled.append(v))
        monkeypatch.setattr(forecast, "run_websocket", lambda: None)
        forecast.run()
        assert enabled == [False]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))