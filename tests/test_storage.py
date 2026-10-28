"""Unit tests for SQLite / PostgreSQL Telemetry Storage Engine.
Author: Ahmad Fathin Naufal (UGM)
"""

import os
import pytest
from src.storage.database import TelemetryDatabase


@pytest.fixture
def temp_db(tmp_path):
    db_file = str(tmp_path / "test_hygiea.db")
    return TelemetryDatabase(db_path=db_file)


def test_database_insert_and_fetch(temp_db):
    sample_records = [
        {
            "timestamp": "2026-03-01 08:00:00",
            "node_id": "ZONE-01-MAIN",
            "node_name": "Main Riser",
            "inlet_pressure_bar": 4.80,
            "outlet_pressure_bar": 4.52,
            "differential_pressure_bar": 0.28,
            "flow_rate_inlet_m3h": 18.5,
            "flow_rate_outlet_m3h": 18.4,
            "flow_discrepancy_m3h": 0.10,
            "acoustic_db": 42.0,
            "vibration_rms_g": 0.045,
            "fluid_temp_c": 24.5,
            "predicted_state": "NOMINAL",
            "severity": "NORMAL",
            "confidence": 0.98,
            "z_score_acoustic": 0.2,
            "z_score_vibration": 0.1,
            "ground_truth_state": "NOMINAL",
        }
    ]

    sample_alerts = [
        {
            "timestamp": "2026-03-01 08:00:00",
            "node_id": "ZONE-01-MAIN",
            "alert_type": "MICRO_LEAK",
            "severity": "CRITICAL",
            "confidence": 0.95,
            "alert_message": "Test leak alert",
        }
    ]

    temp_db.insert_batch(sample_records, sample_alerts)

    df_telemetry = temp_db.fetch_all_telemetry()
    df_alerts = temp_db.fetch_alerts()

    assert len(df_telemetry) == 1
    assert df_telemetry.iloc[0]["node_id"] == "ZONE-01-MAIN"
    assert len(df_alerts) == 1
    assert df_alerts.iloc[0]["severity"] == "CRITICAL"
