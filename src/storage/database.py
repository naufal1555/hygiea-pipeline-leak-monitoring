"""Time-Series Telemetry & Incident Audit Storage Engine.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
import os
import logging

logger = logging.getLogger("hygiea.storage")


class TelemetryDatabase:
    """Manages transactional time-series persistence and analytical querying

    for Hygiea pipeline telemetry.
    """

    def __init__(self, db_path: str = "data/processed/hygiea_telemetry.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        """Initializes normalized relational tables and indexes."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Main high-throughput telemetry table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_stream (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    inlet_pressure_bar REAL NOT NULL,
                    outlet_pressure_bar REAL NOT NULL,
                    differential_pressure_bar REAL NOT NULL,
                    flow_rate_inlet_m3h REAL NOT NULL,
                    flow_rate_outlet_m3h REAL NOT NULL,
                    flow_discrepancy_m3h REAL NOT NULL,
                    acoustic_db REAL NOT NULL,
                    vibration_rms_g REAL NOT NULL,
                    fluid_temp_c REAL NOT NULL,
                    predicted_state TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    z_score_acoustic REAL NOT NULL,
                    z_score_vibration REAL NOT NULL,
                    ground_truth_state TEXT
                );
            """)

            # Incident Alerts Table for Grafana / Operator Alerting
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS incident_alerts (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    alert_message TEXT NOT NULL
                );
            """)

            # Indexes for time-series range scans
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_telemetry_time_node 
                ON telemetry_stream (timestamp, node_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_alerts_severity 
                ON incident_alerts (severity, timestamp);
            """)
            conn.commit()
            logger.info(f"Initialized Hygiea telemetry schema at {self.db_path}")

    def insert_batch(
        self, records: List[Dict[str, Any]], alerts: List[Dict[str, Any]]
    ) -> None:
        """Inserts telemetry records and alert incidents in a single transaction."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            telemetry_tuples = [
                (
                    r["timestamp"],
                    r["node_id"],
                    r["node_name"],
                    r["inlet_pressure_bar"],
                    r["outlet_pressure_bar"],
                    r["differential_pressure_bar"],
                    r["flow_rate_inlet_m3h"],
                    r["flow_rate_outlet_m3h"],
                    r["flow_discrepancy_m3h"],
                    r["acoustic_db"],
                    r["vibration_rms_g"],
                    r["fluid_temp_c"],
                    r["predicted_state"],
                    r["severity"],
                    r["confidence"],
                    r["z_score_acoustic"],
                    r["z_score_vibration"],
                    r.get("ground_truth_state", "UNKNOWN"),
                )
                for r in records
            ]

            cursor.executemany(
                """
                INSERT INTO telemetry_stream (
                    timestamp, node_id, node_name, inlet_pressure_bar, outlet_pressure_bar,
                    differential_pressure_bar, flow_rate_inlet_m3h, flow_rate_outlet_m3h,
                    flow_discrepancy_m3h, acoustic_db, vibration_rms_g, fluid_temp_c,
                    predicted_state, severity, confidence, z_score_acoustic, z_score_vibration,
                    ground_truth_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                telemetry_tuples,
            )

            if alerts:
                alert_tuples = [
                    (
                        a["timestamp"],
                        a["node_id"],
                        a["alert_type"],
                        a["severity"],
                        a["confidence"],
                        a["alert_message"],
                    )
                    for a in alerts
                ]
                cursor.executemany(
                    """
                    INSERT INTO incident_alerts (
                        timestamp, node_id, alert_type, severity, confidence, alert_message
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                    alert_tuples,
                )

            conn.commit()
            logger.info(
                f"Persisted {len(records)} telemetry records and {len(alerts)} alerts to database."
            )

    def fetch_all_telemetry(self) -> pd.DataFrame:
        """Retrieves full telemetry dataframe for reporting or Grafana CSV export."""
        with self._get_connection() as conn:
            return pd.read_sql_query(
                "SELECT * FROM telemetry_stream ORDER BY timestamp ASC", conn
            )

    def fetch_alerts(self, limit: int = 100) -> pd.DataFrame:
        """Retrieves most recent incidents for dashboard table."""
        with self._get_connection() as conn:
            return pd.read_sql_query(
                f"SELECT * FROM incident_alerts ORDER BY timestamp DESC LIMIT {limit}",
                conn,
            )
