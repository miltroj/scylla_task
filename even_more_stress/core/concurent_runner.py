import logging
from concurrent.futures import ThreadPoolExecutor
from statistics import mean, stdev
from typing import List, Tuple

from even_more_stress.core.stress_run_entity import CassandraStressRun
from even_more_stress.core.utils import timestamp_to_stfrtime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class CassandraRunner:
    def __init__(
        self, n_runs: int, node_ip: str, durations: List[int], max_threads: int = 5
    ):
        self.n_runs = n_runs
        self.node_ip = node_ip
        self.durations = durations
        self.max_threads = max_threads

    @staticmethod
    def run_single_cassandra_stress_test(
        thread_id: int, node_ip: str, duration: List[int] | None = None
    ) -> CassandraStressRun:
        single_run = CassandraStressRun(thread_id, node_ip=node_ip, duration=duration)
        return single_run.run_stress_test()

    def run_stress_tests(self) -> List[CassandraStressRun]:
        stress_tests = []

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            for i, duration in zip(range(self.n_runs), self.durations):
                executor.submit(
                    lambda: stress_tests.append(
                        CassandraRunner.run_single_cassandra_stress_test(
                            thread_id=i, node_ip=self.node_ip, duration=duration
                        )
                    )
                )

        return stress_tests

    @staticmethod
    def present_results(
        output_runs: List[CassandraStressRun],
    ) -> Tuple[float, float, float, float]:
        logger.info(f"***Final Results***")
        for run in output_runs:
            logger.info(
                f"Run id='{run.thread_id}' started={timestamp_to_stfrtime(run.start_time)}, "
                f"finished={timestamp_to_stfrtime(run.end_time)} took {run.time_taken:.2f}s!"
            )

        total_op_rate = sum(test.op_rate for test in output_runs)
        avg_latency_mean = mean(test.latency_mean for test in output_runs)
        avg_latency_99th = mean(test.latency_99th for test in output_runs)
        latency_max_stdev = stdev(test.latency_max for test in output_runs)

        logger.info(f"**Summary**")
        logger.info(f"Run N={len(output_runs)} stress runs!")
        logger.info(f"Aggregated Op rate: {total_op_rate:2f} ops/s (sum)")
        logger.info(f"Average Latency mean: {avg_latency_mean:2f} ms")
        logger.info(f"Average Latency 99th percentile: {avg_latency_99th:2f} ms")
        logger.info(f"Latency max standard deviation: {latency_max_stdev:2f} ms")

        return total_op_rate, avg_latency_mean, avg_latency_99th, latency_max_stdev
