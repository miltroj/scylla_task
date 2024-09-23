from even_more_stress.even_more_stress import run_stress_tests, present_results
from even_more_stress.stress_run_entity import CassandraStressRun


def run_fake_process(thread_id, *args, **kwargs):
    return thread_id


class TestingConcurentRunner:

    def test_it_is_posibile_to_run_5_concurent_processes(self):
        return_5_results = run_stress_tests(
            n_runs=5, node_ip="ip-stub", run_function=run_fake_process,
            durations=[10]
        )
        assert len(return_5_results) == 5

    def test_it_is_posibile_to_run_0_concurent_processes(self):
        return_0_results = run_stress_tests(
            n_runs=0, node_ip="ip-stub", run_function=run_fake_process, durations=[10],
        )
        assert len(return_0_results) == 0


class TestMainUtils:

    def test_presenting_end_results(self):
        run_1 = CassandraStressRun(thread_id=1, duration=2, node_ip="111.1.1.1")
        run_1.op_rate = 10
        run_1.latency_mean = 20
        run_1.latency_99th = 30
        run_1.latency_max = 40

        run_2 = CassandraStressRun(thread_id=2, duration=10, node_ip="111.1.1.1")
        run_2.op_rate = 5
        run_2.latency_mean = 6
        run_2.latency_99th = 7
        run_2.latency_max = 8

        total_op_rate, avg_latency_mean, avg_latency_99th, latency_max_stdev = (
            present_results([run_1, run_2])
        )
        assert total_op_rate == 15
        assert avg_latency_mean == 26 / 2
        assert avg_latency_99th == 37 / 2
        assert latency_max_stdev == 22.627416997969522
