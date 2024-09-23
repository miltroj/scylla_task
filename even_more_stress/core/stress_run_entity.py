import logging
import re
from subprocess import PIPE, run
from time import time
from typing import Callable, List

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def map_regexp_first_hit_to_numeric(
    regexp: str, output: str, mapping: Callable
) -> int | float:
    # TODO: fail on no hits
    search = re.search(regexp, output)
    try:
        start = search.group(1)
        end = search.group(2)
    except AttributeError as err:
        logger.error(f"Error {err} while parsing output \n{output}.")
        raise err
    val = f"{start}.{end}"
    return mapping(val)


class CassandraStressRun:
    def __init__(self, thread_id: int, duration: List[int], node_ip: str):
        self.thread_id = thread_id
        self.duration = duration
        self.node_ip = node_ip
        self.op_rate = 0
        self.latency_mean = 0
        self.latency_99th = 0
        self.latency_max = 0
        self.start_time = 0
        self.end_time = 0
        # TODO: pin specific digest
        self._image_name = "scylladb/cassandra-stress"
        self._cassandra_threads = 10
        self._run_command = (
            f"docker run {self._image_name} 'cassandra-stress write duration={self.duration}s "
            f"-rate threads={self._cassandra_threads} -node {self.node_ip}'"
        )

    @property
    def time_taken(self) -> time:
        return self.end_time - self.start_time

    def run_stress_test(self):
        self.start_time = time()
        logging.info(
            f"Thread {self.thread_id}: Starting stress test with duration={self.duration} on node {self.node_ip}"
        )
        result = run(self._run_command, stdout=PIPE, stderr=PIPE, shell=True)
        if result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode}")
            raise Exception(f"StdError {result.stderr}, {result.stdout}!")
        self.end_time = time()
        self.parse_results(result.stdout.decode("utf-8"))
        logging.info(
            f"Thread {self.thread_id}: Completed stress test. Duration: {self.time_taken:.2f} seconds"
        )
        return self

    @staticmethod
    def _match_op_rate(output) -> float:
        return map_regexp_first_hit_to_numeric(
            regexp=r"Op rate\s*:\s*(\d+),(\d+)", output=output, mapping=float
        )

    @staticmethod
    def _match_latency_mean(output) -> float:
        return map_regexp_first_hit_to_numeric(
            regexp=r"Latency mean\s*:\s*(\d+).(\d+)", output=output, mapping=float
        )

    @staticmethod
    def _match_latency_99th(output) -> float:
        return map_regexp_first_hit_to_numeric(
            regexp=r"Latency 99th percentile\s*:\s*(\d+).(\d+)",
            output=output,
            mapping=float,
        )

    @staticmethod
    def _match_latency_max(output) -> float:
        return map_regexp_first_hit_to_numeric(
            regexp=r"Latency max\s*:\s*(\d+).(\d+)", output=output, mapping=float
        )

    def parse_results(self, output: str) -> None:
        self.op_rate = self._match_op_rate(output)
        self.latency_mean = self._match_latency_mean(output)
        self.latency_99th = self._match_latency_99th(output)
        self.latency_max = self._match_latency_max(output)
        logging.debug(
            f"Thread {self.thread_id}: Parsed Results - Op rate: {self.op_rate}, Latency mean: {self.latency_mean}, "
            f"Latency 99th: {self.latency_99th}, Latency max: {self.latency_max}"
        )

    def __repr__(self) -> str:
        return (
            f"Run {self.thread_id=} - {self.op_rate=}, {self.latency_mean=}, "
            f"{self.latency_99th=}, {self.latency_max=}"
        )
