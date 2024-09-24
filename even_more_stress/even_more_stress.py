import logging
from typing import List, Tuple

from even_more_stress.core.arg_parser import parse_args
from even_more_stress.core.concurent_runner import CassandraRunner
from even_more_stress.core.utils import (
    generate_html_report,
    scylla_cluster_should_exist,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def run_stress_tests_and_present_results(
    n_runs: int, node_ip: str, durations: List[int], max_threads: int
) -> Tuple[float, float, float, float]:
    runner = CassandraRunner(n_runs, node_ip, durations, max_threads)
    results = runner.run_stress_tests()
    return runner.present_results(results)


def main(argv: List[str] | None = None):
    args = parse_args()
    logger.info("Even more stress STARTING!")
    scylla_cluster_should_exist()
    results = run_stress_tests_and_present_results(
        args.N_runs, args.node_ip, args.durations, args.max_threads
    )
    if args.html:
        generate_html_report(results)


if __name__ == "__main__":
    main()
