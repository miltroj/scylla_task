import argparse
import logging
import threading
from statistics import mean, stdev
from even_more_stress.stress_run import CassandraStressRun
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def run_single_cassandra_stress_test(thread_id, node_ip, duration=None):
    single_run = CassandraStressRun(thread_id, node_ip=node_ip, duration=duration)
    return single_run.run_stress_test()


def present_results(output_runs):
    logger.info(f"Run N={len(output_runs)} stress runs!")
    for run in output_runs:
        # TODO: reduce precision
        logger.info(
            f"Run id='{run.thread_id}' started={datetime.fromtimestamp(run.start_time)}, "
            f"finished={datetime.fromtimestamp(run.end_time)} took {run.time_taken:.2f}s!"
        )

    total_op_rate = sum(test.op_rate for test in output_runs)
    avg_latency_mean = mean(test.latency_mean for test in output_runs)
    avg_latency_99th = mean(test.latency_99th for test in output_runs)
    # TODO: handle single record
    latency_max_stdev = stdev(test.latency_max for test in output_runs)

    logger.info(f"Aggregated Op rate: {total_op_rate} ops/s (sum)")
    logger.info(f"Average Latency mean: {avg_latency_mean} ms")
    logger.info(f"Average Latency 99th percentile: {avg_latency_99th} ms")
    logger.info(f"Latency max standard deviation: {latency_max_stdev} ms")

    return total_op_rate, avg_latency_mean, avg_latency_99th, latency_max_stdev


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run cassandra-stress tests and analyze results."
    )
    parser.add_argument(
        "--N_runs",
        type=int,
        required=True,
        help="Number of concurrent cassandra stress containers.",
    )
    parser.add_argument(
        "--durations",
        type=int,
        nargs="+",
        required=False,
        default=None,
        help="List of durations (in seconds) for each stress command. Must match the number of threads.",
    )
    parser.add_argument(
        "--node_ip", required=True, help="Node IP of the ScyllaDB instance."
    )
    return parser.parse_args()


def run_stress_tests(
    n_runs, node_ip, durations=None, run_function=run_single_cassandra_stress_test
):
    stress_tests = []
    threads = []

    # TODO: handle empty duration
    for i, duration in zip(range(n_runs), durations):
        thread = threading.Thread(
            target=lambda: stress_tests.append(
                run_function(thread_id=i, node_ip=node_ip, duration=duration)
            )
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return stress_tests


if __name__ == "__main__":

    args = parse_args()
    logger.info("Even more stress STARTING!")

    if args.durations and args.N_runs != len(args.durations):
        raise ValueError(
            f"N_runs and durations should be the same while N_runs={args.N_runs}, durations={len(args.durations)}!"
        )
    results = run_stress_tests(
        n_runs=args.N_runs, durations=args.durations, node_ip=args.node_ip
    )
    present_results(results)
