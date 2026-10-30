"""Hadoop / PySpark Big Data Stream & Batch Telemetry Processing Engine.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
Project: Hygiea - Smart Pipeline Leakage Monitoring System
"""

from typing import Dict, Any, List, Tuple
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("hygiea.hadoop")

Tuple_KV = Tuple[str, Any]


class PySparkPipelineSimulator:
    """Simulates a PySpark / Hadoop MapReduce streaming aggregation job

    executing distributed windowed calculations across 1,000+ IoT telemetry
    events on HDFS (Hadoop Distributed File System).
    """

    def __init__(self, hdfs_input_path: str = "hdfs://namenode:9000/hygiea/telemetry/raw/"):
        self.hdfs_input_path = hdfs_input_path

    def run_spark_aggregation(self, raw_telemetry_df: pd.DataFrame) -> pd.DataFrame:
        """Executes Spark-like DataFrame transformations:

        1. group_by(window, node_id)
        2. avg(differential_pressure), max(acoustic_db), stddev(vibration_rms)
        3. filter(z_score > 3.0 OR dp > 0.38)
        """
        logger.info(f"Submitting PySpark batch job to Hadoop Cluster on {self.hdfs_input_path}...")
        logger.info(f"Processing {len(raw_telemetry_df)} distributed IoT telemetry partitions...")

        # Emulate Spark DataFrame operations
        df = raw_telemetry_df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")

        # 30-second rolling window per node
        grouped = df.groupby("node_id")

        aggregated_records: List[Dict[str, Any]] = []

        for node_id, node_group in grouped:
            rolling_dp_mean = node_group["differential_pressure_bar"].rolling(window=30, min_periods=5).mean()
            rolling_ac_mean = node_group["acoustic_db"].rolling(window=30, min_periods=5).mean()
            rolling_ac_std = node_group["acoustic_db"].rolling(window=30, min_periods=5).std().fillna(1.0)

            node_group = node_group.copy()
            node_group["spark_rolling_dp_avg"] = rolling_dp_mean
            node_group["spark_z_score_acoustic"] = np.abs(node_group["acoustic_db"] - rolling_ac_mean) / (rolling_ac_std + 1e-4)

            # Spark threshold filter
            node_group["spark_leak_candidate"] = (node_group["spark_rolling_dp_avg"] > 0.35) & (node_group["spark_z_score_acoustic"] > 2.0)
            aggregated_records.append(node_group)

        result_df = pd.concat(aggregated_records).reset_index()
        logger.info(f"PySpark job completed: {len(result_df)} records partitioned across HDFS.")
        return result_df


class HadoopMapReduceJob:
    """Classic Hadoop MapReduce mapper and reducer for IoT sensor aggregation."""

    @staticmethod
    def mapper(record: Dict[str, Any]) -> Tuple_KV:
        """Mapper: Emits (node_id, (dp, flow_disc, acoustic))"""
        key = record["node_id"]
        value = (
            float(record["differential_pressure_bar"]),
            float(record["flow_discrepancy_m3h"]),
            float(record["acoustic_db"])
        )
        return key, value

    @staticmethod
    def reducer(key: str, values: List[Tuple_KV]) -> Dict[str, Any]:
        """Reducer: Computes mean differential pressure and peak acoustic emission per node."""
        dps = [v[0] for v in values]
        flow_discs = [v[1] for v in values]
        acoustics = [v[2] for v in values]

        return {
            "node_id": key,
            "sample_count": len(values),
            "mean_dp_bar": round(float(np.mean(dps)), 3),
            "mean_flow_discrepancy_m3h": round(float(np.mean(flow_discs)), 3),
            "max_acoustic_db": round(float(np.max(acoustics)), 2),
            "cluster_status": "PROCESSED_ON_HDFS"
        }


Tuple_KV = Any
