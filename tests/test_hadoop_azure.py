"""Unit tests for Hadoop PySpark processing and Azure Cloud components.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

import json
import pytest
import pandas as pd
from hadoop.pyspark_leak_stream import PySparkPipelineSimulator, HadoopMapReduceJob


def test_hadoop_mapreduce_job():
    records = [
        {"node_id": "ZONE-01-MAIN", "differential_pressure_bar": 0.28, "flow_discrepancy_m3h": 0.1, "acoustic_db": 42.0},
        {"node_id": "ZONE-01-MAIN", "differential_pressure_bar": 0.30, "flow_discrepancy_m3h": 0.12, "acoustic_db": 44.0},
        {"node_id": "ZONE-01-MAIN", "differential_pressure_bar": 0.65, "flow_discrepancy_m3h": 0.55, "acoustic_db": 68.0},
    ]

    mapped = [HadoopMapReduceJob.mapper(r) for r in records]
    assert len(mapped) == 3
    assert mapped[0][0] == "ZONE-01-MAIN"

    # Group by key
    key = "ZONE-01-MAIN"
    values = [m[1] for m in mapped]
    reduced = HadoopMapReduceJob.reducer(key, values)

    assert reduced["node_id"] == "ZONE-01-MAIN"
    assert reduced["sample_count"] == 3
    assert reduced["max_acoustic_db"] == 68.0
    assert reduced["cluster_status"] == "PROCESSED_ON_HDFS"


def test_azure_arm_template_validity():
    with open("azure/azure_infrastructure_arm.json", "r", encoding="utf-8") as f:
        template = json.load(f)

    assert template["$schema"] is not None
    assert "resources" in template
    resource_types = [r["type"] for r in template["resources"]]
    assert "Microsoft.Devices/IotHubs" in resource_types
    assert "Microsoft.Storage/storageAccounts" in resource_types
    assert "Microsoft.StreamAnalytics/streamingjobs" in resource_types
