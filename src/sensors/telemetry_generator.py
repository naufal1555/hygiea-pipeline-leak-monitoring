"""IoT Sensor Telemetry Simulator with Fluid Dynamic & Cavitation Physics.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Any
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger("hygiea.sensors")


@dataclass
class TelemetrySample:
    timestamp: str
    node_id: str
    node_name: str
    inlet_pressure_bar: float
    outlet_pressure_bar: float
    differential_pressure_bar: float
    flow_rate_inlet_m3h: float
    flow_rate_outlet_m3h: float
    flow_discrepancy_m3h: float
    acoustic_db: float
    vibration_rms_g: float
    fluid_temp_c: float
    ground_truth_state: str  # NOMINAL, MICRO_LEAK, PRESSURE_SURGE


class TelemetryGenerator:
    """Generates synthetic high-fidelity IoT time-series telemetry adhering to

    fluid mechanics (Darcy-Weisbach head loss, orifice cavitation, acoustic
    emission).
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.zones = config["pipeline_zones"]
        self.sim_cfg = config["simulation"]
        np.random.seed(config["system"].get("random_seed", 42))

    def generate_dataset(self) -> pd.DataFrame:
        """Generates continuous time-series telemetry across all configured

        pipeline nodes.
        """
        samples_per_node = self.sim_cfg.get("samples_per_node", 350)
        leak_prob = self.sim_cfg.get("micro_leak_probability", 0.05)
        surge_prob = self.sim_cfg.get("pressure_surge_probability", 0.03)

        records: List[Dict[str, Any]] = []
        base_time = datetime(2026, 3, 1, 8, 0, 0)

        for zone in self.zones:
            node_id = zone["zone_id"]
            node_name = zone["name"]
            p_in_base = zone["nominal_inlet_pressure_bar"]
            p_out_base = zone["nominal_outlet_pressure_bar"]
            flow_base = zone["nominal_flow_rate_m3h"]
            acoust_base = zone["baseline_acoustic_db"]
            vib_base = zone["baseline_vibration_rms_g"]

            # Persistent leak state tracker for realistic burst / fissure duration
            leak_active_ticks = 0
            surge_active_ticks = 0

            for tick in range(samples_per_node):
                current_time = base_time + timedelta(
                    seconds=tick * self.sim_cfg.get("sample_frequency_sec", 1.0)
                )

                # Diurnal fluid flow oscillation
                oscillation = 0.08 * np.sin(tick * 0.05)
                f_in = flow_base * (1.0 + oscillation) + np.random.normal(
                    0, 0.05 * flow_base
                )
                p_in = p_in_base + np.random.normal(0, 0.04)

                # Nominal frictional differential pressure drop
                nominal_dp = p_in_base - p_out_base
                p_out = p_in - nominal_dp + np.random.normal(0, 0.02)
                f_out = f_in + np.random.normal(0, 0.02)

                acoustic = acoust_base + np.random.normal(0, 1.2)
                vibration = vib_base + np.random.normal(0, 0.003)
                temp = 24.5 + 0.5 * np.sin(tick * 0.02) + np.random.normal(0, 0.1)

                state = "NOMINAL"

                # Evaluate state transitions
                if leak_active_ticks > 0:
                    leak_active_ticks -= 1
                    state = "MICRO_LEAK"
                elif surge_active_ticks > 0:
                    surge_active_ticks -= 1
                    state = "PRESSURE_SURGE"
                else:
                    # Random injection trigger
                    rand_val = np.random.rand()
                    if rand_val < leak_prob and tick > 30:
                        leak_active_ticks = np.random.randint(
                            12, 28
                        )  # Leak persists for multiple frames
                        state = "MICRO_LEAK"
                    elif rand_val < leak_prob + surge_prob and tick > 30:
                        surge_active_ticks = np.random.randint(3, 8)
                        state = "PRESSURE_SURGE"

                # Apply physical mutations based on thermodynamic state
                if state == "MICRO_LEAK":
                    leak_severity = np.random.uniform(0.35, 0.85)  # m3/h leak
                    f_out = max(0.5, f_in - leak_severity)
                    p_out -= np.random.uniform(0.42, 0.75)  # bar drop
                    acoustic += np.random.uniform(
                        14.0, 28.0
                    )  # Orifice cavitation hiss
                    vibration += np.random.uniform(
                        0.045, 0.110
                    )  # Structural turbulence

                elif state == "PRESSURE_SURGE":
                    p_in += np.random.uniform(1.20, 2.10)  # Water hammer shock
                    p_out += np.random.uniform(0.80, 1.50)
                    vibration += np.random.uniform(
                        0.090, 0.220
                    )  # Mechanical shudder
                    acoustic += np.random.uniform(6.0, 15.0)

                dp = max(0.01, p_in - p_out)
                f_disc = max(0.0, f_in - f_out)

                sample = TelemetrySample(
                    timestamp=current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    node_id=node_id,
                    node_name=node_name,
                    inlet_pressure_bar=round(float(p_in), 3),
                    outlet_pressure_bar=round(float(p_out), 3),
                    differential_pressure_bar=round(float(dp), 3),
                    flow_rate_inlet_m3h=round(float(f_in), 3),
                    flow_rate_outlet_m3h=round(float(f_out), 3),
                    flow_discrepancy_m3h=round(float(f_disc), 3),
                    acoustic_db=round(float(acoustic), 2),
                    vibration_rms_g=round(float(vibration), 4),
                    fluid_temp_c=round(float(temp), 2),
                    ground_truth_state=state,
                )
                records.append(asdict(sample))

        df = pd.DataFrame(records)
        logger.info(
            f"Generated {len(df)} telemetry samples across {len(self.zones)} pipeline nodes."
        )
        return df
