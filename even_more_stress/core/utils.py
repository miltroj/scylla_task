import logging
from datetime import datetime
from html import escape
from subprocess import PIPE, run
from typing import List

from even_more_stress.core.stress_run_entity import CassandraStressRun

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def scylla_cluster_should_exist() -> None:
    result = run(
        f"docker exec some-scylla nodetool status", stdout=PIPE, stderr=PIPE, shell=True
    )
    if result.returncode != 0:
        logger.error("Failed to get cluster status.")
        raise Exception(f"Error: {result.stderr.decode('utf-8')}")
    logger.info(f"Cluster Running!")


def generate_html_report(output_runs: List[CassandraStressRun]) -> None:
    html_content = "<html><head><title>Stress Test Results</title></head><body>"
    html_content += "<h1>Stress Test Results</h1>"
    for run in output_runs:
        html_content += (
            f"<p>Run ID: {escape(str(run.thread_id))}, Op rate: {run.op_rate}, "
            f"Latency mean: {run.latency_mean}</p>"
        )
    html_content += "</body></html>"
    with open("stress_test_results.html", "w") as file:
        file.write(html_content)


def timestamp_to_stfrtime(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
