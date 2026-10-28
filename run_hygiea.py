"""Master entry point for Hygiea Pipeline Leak Monitoring.
Author: Ahmad Fathin Naufal (UGM) - Big Data & Server Engineer
"""

import sys
import logging
from src.pipeline.orchestrator import HygieaPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def main():
    pipeline = HygieaPipeline("config/config.yaml")
    metrics = pipeline.run()
    print("\n" + "=" * 60)
    print("HYGIEA PIPELINE EXECUTION VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"Total Processed IoT Samples : {metrics['total_samples']}")
    print(f"Micro-Leak Recall           : {metrics['leak_recall']}%")
    print(f"Micro-Leak Precision        : {metrics['leak_precision']}%")
    print(f"False Alarm Rate (FAR)      : {metrics['false_alarm_rate']}%")
    print(f"Overall Accuracy            : {metrics['accuracy']}%")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
