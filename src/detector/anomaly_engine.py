"""Dual-Tier Big Data Telemetry Anomaly Detection Engine.
Combines Hydraulic Gradient Mass-Balance with Sliding-Window Z-Score.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

from collections import deque
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional
import numpy as np
import logging

logger = logging.getLogger("hygiea.detector")


@dataclass
class AnomalyResult:
    predicted_state: str  # NOMINAL, MICRO_LEAK, PRESSURE_SURGE, TRANSIENT_WARNING
    severity: str  # NORMAL, INFO, WARNING, CRITICAL
    confidence: float
    z_score_acoustic: float
    z_score_vibration: float
    hydraulic_leak_flag: bool
    surge_flag: bool
    alert_message: str


class RollingStats:
    """Maintains fixed sliding-window statistics for streaming telemetry."""

    def __init__(self, window_size: int = 45):
        self.window_size = window_size
        self.acoustic_buf = deque(maxlen=window_size)
        self.vibration_buf = deque(maxlen=window_size)
        self.dp_buf = deque(maxlen=window_size)

    def update(self, acoustic: float, vibration: float, dp: float) -> None:
        self.acoustic_buf.append(acoustic)
        self.vibration_buf.append(vibration)
        self.dp_buf.append(dp)

    def get_z_scores(
        self, acoustic: float, vibration: float
    ) -> Tuple[float, float]:
        if len(self.acoustic_buf) < 10:
            return 0.0, 0.0

        mu_ac = float(np.mean(self.acoustic_buf))
        std_ac = float(np.std(self.acoustic_buf)) + 1e-4

        mu_vib = float(np.mean(self.vibration_buf))
        std_vib = float(np.std(self.vibration_buf)) + 1e-5

        z_ac = abs(acoustic - mu_ac) / std_ac
        z_vib = abs(vibration - mu_vib) / std_vib
        return round(z_ac, 2), round(z_vib, 2)


class AnomalyEngine:
    """Industrial Pipeline Anomaly Engine implementing Mass-Balance & Z-Score

    fusion.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        thresholds = config["detection_thresholds"]
        self.z_ac_warn = thresholds.get("z_score_acoustic_warning", 2.5)
        self.z_ac_crit = thresholds.get("z_score_acoustic_critical", 3.2)
        self.z_vib_crit = thresholds.get("z_score_vibration_critical", 3.0)
        self.dp_thresh = thresholds.get(
            "differential_pressure_leak_drop_bar", 0.38
        )
        self.flow_disc_thresh = thresholds.get(
            "mass_balance_divergence_m3h", 0.28
        )
        self.window_size = thresholds.get("sliding_window_size", 45)

        # Per-node rolling buffer map
        self.node_buffers: Dict[str, RollingStats] = {}

    def _get_buffer(self, node_id: str) -> RollingStats:
        if node_id not in self.node_buffers:
            self.node_buffers[node_id] = RollingStats(
                window_size=self.window_size
            )
        return self.node_buffers[node_id]

    def process_telemetry(self, row: Dict[str, Any]) -> AnomalyResult:
        """Evaluates a single telemetry frame against hydraulic and statistical

        criteria.
        """
        node_id = row["node_id"]
        p_in = float(row["inlet_pressure_bar"])
        p_out = float(row["outlet_pressure_bar"])
        dp = float(row["differential_pressure_bar"])
        flow_disc = float(row["flow_discrepancy_m3h"])
        ac = float(row["acoustic_db"])
        vib = float(row["vibration_rms_g"])

        buf = self._get_buffer(node_id)
        z_ac, z_vib = buf.get_z_scores(ac, vib)

        # Hydraulic condition checks
        hydraulic_leak = (dp >= self.dp_thresh) and (
            flow_disc >= self.flow_disc_thresh
        )
        surge_detected = (p_in >= 5.50) or (
            z_vib >= self.z_vib_crit and flow_disc < self.flow_disc_thresh
        )

        # Classification decision matrix
        if hydraulic_leak and (z_ac >= 2.0 or z_vib >= 2.0):
            state = "MICRO_LEAK"
            severity = "CRITICAL"
            confidence = min(0.99, 0.85 + 0.05 * min(z_ac, 3.0))
            msg = f"CRITICAL: Pipeline fissure detected! dp={dp:.2f} bar, mass loss={flow_disc:.2f} m3/h, z_ac={z_ac:.1f}"

        elif surge_detected:
            state = "PRESSURE_SURGE"
            severity = "WARNING"
            confidence = 0.92
            msg = f"WARNING: Dynamic pressure surge detected! P_in={p_in:.2f} bar, z_vib={z_vib:.1f}"

        elif z_ac >= self.z_ac_warn or dp >= (self.dp_thresh * 0.85):
            state = "TRANSIENT_WARNING"
            severity = "INFO"
            confidence = 0.72
            msg = f"INFO: Elevated cavitation hiss or minor flow impedance detected on {node_id}."

        else:
            state = "NOMINAL"
            severity = "NORMAL"
            confidence = 0.98
            msg = "System operating within nominal steady-state fluid envelope."

        # Update statistical baseline with nominal-range samples to prevent window contamination
        if state in ("NOMINAL", "TRANSIENT_WARNING"):
            buf.update(ac, vib, dp)

        return AnomalyResult(
            predicted_state=state,
            severity=severity,
            confidence=round(confidence, 3),
            z_score_acoustic=z_ac,
            z_score_vibration=z_vib,
            hydraulic_leak_flag=bool(hydraulic_leak),
            surge_flag=bool(surge_detected),
            alert_message=msg,
        )
