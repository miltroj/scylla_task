import pytest

from even_more_stress.core.stress_run_entity import CassandraStressRun


@pytest.fixture
def cassandra_stress_output():
    return """Results:
Op rate                   :   15,156 op/s  [WRITE: 15,156 op/s]
Partition rate            :   15,156 pk/s  [WRITE: 15,156 pk/s]
Row rate                  :   15,156 row/s [WRITE: 15,156 row/s]
Latency mean              :    0.6 ms [WRITE: 0.6 ms]
Latency median            :    0.6 ms [WRITE: 0.6 ms]
Latency 95th percentile   :    0.9 ms [WRITE: 0.9 ms]
Latency 99th percentile   :    1.2 ms [WRITE: 1.2 ms]
Latency 99.9th percentile :    2.4 ms [WRITE: 2.4 ms]
Latency max               :   17.3 ms [WRITE: 17.3 ms]
Total partitions          :    164,042 [WRITE: 164,042]
Total errors              :          0 [WRITE: 0]
Total GC count            : 0
Total GC memory           : 0.000 KiB
Total GC time             :    0.0 seconds
Avg GC time               :    NaN ms
StdDev GC time            :    0.0 ms
Total operation time      : 00:00:10""".replace(
        "\n", ""
    ).replace(
        "\r", ""
    )


class TestOutputRegexpMatching:

    def test_should_parse_op_rate(self, cassandra_stress_output):
        assert CassandraStressRun._match_op_rate(cassandra_stress_output) == 15.156

    def test_should_match_latency_mean(self, cassandra_stress_output):
        assert CassandraStressRun._match_latency_mean(cassandra_stress_output) == 0.6

    def test_should_match_latency_99th_match(self, cassandra_stress_output):
        assert CassandraStressRun._match_latency_99th(cassandra_stress_output) == 1.2

    def test_should_match_latency_max(self, cassandra_stress_output):
        assert CassandraStressRun._match_latency_max(cassandra_stress_output) == 17.3

    def test_parsing_all_output_results_to_instance_run(self, cassandra_stress_output):
        run = CassandraStressRun("thread_id", "duration", "node_ip")
        run.parse_results(cassandra_stress_output)
        assert run.op_rate == 15.156
        assert run.latency_mean == 0.6
        assert run.latency_99th == 1.2
        assert run.latency_max == 17.3


class TestCassandraRunHelperMethods:

    def test_run_command_builds_correctly(self):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        assert "duration=2s" in run._run_command
        assert "-node 111.0.0.1" in run._run_command


class TestCassandraRunEntity:

    def test_single_run_accumulates_results_positive(self, cassandra_stress_output):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        run._run_command = f'echo "{cassandra_stress_output}"'

        run.run_stress_test()
        assert run.op_rate == 15.156
        assert run.latency_mean == 0.6
        assert run.latency_99th == 1.2
        assert run.latency_max == 17.3

    def test_single_run_fails_on_no_results(self):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        run._run_command = f'echo "EMPTY"'
        with pytest.raises(AttributeError) as err:
            run.run_stress_test()
