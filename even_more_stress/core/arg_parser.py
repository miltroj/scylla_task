import argparse
from typing import List

DEFAULT_TEST_DURATION = 10


def positive_int(value: str) -> int:
    try:
        int_value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not an integer!")
    if int_value < 1:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer!")
    return int_value


def parse_args(argv: List[str] | None = None) -> argparse:
    parser = argparse.ArgumentParser(
        description="Run cassandra-stress tests and analyze results."
    )
    parser.add_argument(
        "--N_runs",
        type=positive_int,
        required=True,
        help="Number of concurrent cassandra stress containers.",
    )
    parser.add_argument(
        "--durations",
        type=positive_int,
        nargs="+",
        required=False,
        default=[],
        help="List of durations (in seconds) for each stress command. Must match the number of threads.",
    )
    parser.add_argument(
        "--node_ip", required=True, help="Node IP of the ScyllaDB instance."
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Generate an HTML report of the stress test results.",
    )
    parser.add_argument(
        "--max_threads",
        type=positive_int,
        default=5,
        required=False,
        help="Max number of threads to run concurrently.",
    )
    args = parser.parse_args(argv)

    if not args.durations:
        args.durations = [DEFAULT_TEST_DURATION] * args.N_runs
    if args.N_runs != len(args.durations):
        raise ValueError(
            f"N_runs and durations should be the same while N_runs={args.N_runs}, durations={len(args.durations)}!"
        )
    return args
