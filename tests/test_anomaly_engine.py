"""Unit tests for Dual-Tier Anomaly Engine (Hydraulic Gradient & Z-Score).
Author: Ahmad Fathin Naufal (UGM)
"""

import pytest
import yaml
from src.detector.anomaly_engine import AnomalyEngine, RollingStats


@pytest.fixture
def detector_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_rolling_stats_z_score():
    stats = RollingStats(window_size=30)
    # Populate nominal samples
    for _ in range(25):
        stats.update(acoustic=40.0, vibration=0.05, dp=0.25)

    # Spike acoustic
    z_ac, z_vib = stats.get_z_scores(acoustic=65.0, vibration=0.05)
    assert z_ac > 2.5, f"Expected high Z-Score on acoustic spike, got {z_ac}"


def test_anomaly_classification_leak(detector_config):
    engine = AnomalyEngine(detector_config)

    # Populate baseline buffer
    for _ in range(20):
        engine.process_telemetry(
            {
                "node_id": "ZONE-01-MAIN",
                "inlet_pressure_bar": 4.80,
                "outlet_pressure_bar": 4.52,
                "differential_pressure_bar": 0.28,
                "flow_rate_inlet_m3h": 18.5,
                "flow_rate_outlet_m3h": 18.4,
                "flow_discrepancy_m3h": 0.1,
                "acoustic_db": 42.0,
                "vibration_rms_g": 0.045,
            }
        )

    # Inject critical micro-leak frame
    leak_sample = {
        "node_id": "ZONE-01-MAIN",
        "inlet_pressure_bar": 4.75,
        "outlet_pressure_bar": 4.10,
        "differential_pressure_bar": 0.65,  # Exceeds 0.38 threshold
        "flow_rate_inlet_m3h": 18.5,
        "flow_rate_outlet_m3h": 17.8,
        "flow_discrepancy_m3h": 0.70,  # Exceeds 0.28 threshold
        "acoustic_db": 68.0,  # Elevated cavitation hiss
        "vibration_rms_g": 0.12,
    }

    result = engine.process_telemetry(leak_sample)
    assert result.predicted_state == "MICRO_LEAK"
    assert result.severity == "CRITICAL"
    assert result.hydraulic_leak_flag is True
