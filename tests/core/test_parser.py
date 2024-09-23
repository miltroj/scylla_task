import pytest

from even_more_stress.core.arg_parser import parse_args


class TestParsingArgs:

    def test_should_parsing_mandatory_args(self):
        args = parse_args(["--N_runs", "5", "--node_ip", "111.222.333.444"])
        assert args.N_runs == 5
        assert args.node_ip == "111.222.333.444"

    def test_should_parsing_optional_args(self):
        args = parse_args(
            [
                "--N_runs",
                "5",
                "--node_ip",
                "111.222.333.444",
                "--max_threads",
                "10",
                "--durations",
                "1",
                "2",
                "3",
                "4",
                "5",
            ]
        )
        assert args.max_threads == 10
        assert args.durations == [1, 2, 3, 4, 5]

    def test_should_fail_on_N_runs_durations_mismatch(self):
        with pytest.raises(ValueError):
            parse_args(
                [
                    "--N_runs",
                    "5",
                    "--node_ip",
                    "111.222.333.444",
                    "--durations",
                    "1",
                    "2",
                    "3",
                    "4",
                ]
            )

    def test_should_fail_on_negative_N_runs(self):
        with pytest.raises(SystemExit):
            parse_args(["--N_runs", "-5", "--node_ip", "111.222.333.444"])

    def test_should_fail_on_negative_max_threads(self):
        with pytest.raises(SystemExit):
            parse_args(
                [
                    "--N_runs",
                    "5",
                    "--node_ip",
                    "111.222.333.444",
                    "--max_threads",
                    "-10",
                ]
            )

    def test_should_fail_on_negative_duration(self):
        with pytest.raises(SystemExit):
            parse_args(
                [
                    "--N_runs",
                    "5",
                    "--node_ip",
                    "111.222.333.444",
                    "--durations",
                    "-1",
                    "2",
                    "3",
                    "4",
                    "5",
                ]
            )

    def test_should_fail_on_non_int_duration(self):
        with pytest.raises(SystemExit):
            parse_args(
                [
                    "--N_runs",
                    "5",
                    "--node_ip",
                    "111.222.333.444",
                    "--durations",
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "a",
                ]
            )
