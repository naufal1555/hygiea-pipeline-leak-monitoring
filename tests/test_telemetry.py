"""Unit tests for IoT Telemetry Generator and Fluid Mechanics Constraints.
Author: Ahmad Fathin Naufal (UGM)
"""

import pytest
import yaml
import pandas as pd
from src.sensors.telemetry_generator import TelemetryGenerator


@pytest.fixture
def test_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_telemetry_generation_volume(test_config):
    generator = TelemetryGenerator(test_config)
    df = generator.generate_dataset()

    # Must strictly exceed the CV claim of 1,000+ IoT samples
    assert len(df) >= 1000, f"Expected >= 1,000 samples, got {len(df)}"
    assert "node_id" in df.columns
    assert "differential_pressure_bar" in df.columns
    assert "flow_discrepancy_m3h" in df.columns
    assert "acoustic_db" in df.columns


def test_hydraulic_physics_sanity(test_config):
    generator = TelemetryGenerator(test_config)
    df = generator.generate_dataset()

    # Pressure must always be non-negative
    assert (df["inlet_pressure_bar"] > 0).all()
    assert (df["outlet_pressure_bar"] > 0).all()

    # Differential pressure must be positive
    assert (df["differential_pressure_bar"] >= 0).all()

    # Flow rates must be non-negative
    assert (df["flow_rate_inlet_m3h"] > 0).all()
    assert (df["flow_rate_outlet_m3h"] > 0).all()
