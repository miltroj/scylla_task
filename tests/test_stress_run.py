from even_more_stress.stress_run_entity import CassandraStressRun
import pytest


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
Total operation time      : 00:00:10""".replace("\n", "").replace("\r", "")


@pytest.fixture
def cassandra_full_logs():
    return """******************** Stress Settings ********************
Command:
  Type: write
  Count: -1
  Duration: 10 SECONDS
  No Warmup: false
  Consistency Level: LOCAL_ONE
  Serial Consistency Level: SERIAL
  Target Uncertainty: not applicable
  Key Size (bytes): 10
  Counter Increment Distibution: add=fixed(1)
Rate:
  Auto: false
  Thread Count: 10
  OpsPer Sec: 0
Population:
  Sequence: 1..1000000
  Order: ARBITRARY
  Wrap: true
Insert:
  Revisits: Uniform:  min=1,max=1000000
  Visits: Fixed:  key=1
  Row Population Ratio: Ratio: divisor=1.000000;delegate=Fixed:  key=1
  Batch Type: not batching
Columns:
  Max Columns Per Key: 5
  Column Names: [C0, C1, C2, C3, C4]
  Comparator: AsciiType
  Timestamp: null
  Variable Column Count: false
  Slice: false
  Size Distribution: Fixed:  key=34
  Count Distribution: Fixed:  key=5
Errors:
  Ignore: false
  Tries: 10
Log:
  No Summary: false
  No Settings: false
  File: null
  Interval Millis: 1000
  Level: NORMAL
Mode:
  API: JAVA_DRIVER_NATIVE
  Connection Style: CQL_PREPARED
  CQL Version: CQL3
  Protocol Version: V4
  Username: null
  Password: null
  Auth Provide Class: null
  Max Pending Per Connection: null
  Connections Per Host: 8
  Compression: NONE
Node:
  Nodes: [172.17.0.2]
  Is White List: false
  Datacenter: null
  Rack: null
Schema:
  Keyspace: keyspace1
  Replication Strategy: org.apache.cassandra.locator.NetworkTopologyStrategy
  Replication Strategy Options: {replication_factor=1}
  Storage Options: {}
  Table Compression: null
  Table Compaction Strategy: null
  Table Compaction Strategy Options: {}
Transport:
  factory=org.apache.cassandra.thrift.TFramedTransportFactory; truststore=null; truststore-password=null; keystore=null; keystore-password=null; ssl-protocol=TLS; ssl-alg=SunX509; store-type=JKS; ssl-ciphers=TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA;
Port:
  Native Port: 9042
  Thrift Port: 9160
  JMX Port: 7199
Send To Daemon:
  *not set*
Graph:
  File: null
  Revision: unknown
  Title: null
  Operation: WRITE
TokenRange:
  Wrap: false
  Split Factor: 1
CloudConf:
  File: null

INFO  [main] 2024-09-22 14:15:57,941 GuavaCompatibility.java:204 - Detected Guava >= 19 in the classpath, using modern compatibility layer
INFO  [main] 2024-09-22 14:15:57,952 Cluster.java:280 - DataStax Java driver 3.11.5.3 for Apache Cassandra
INFO  [main] 2024-09-22 14:15:58,000 Native.java:113 - Could not load JNR C Library, native system calls through this library will not be available (set this logger level to DEBUG to see the full stack trace).
INFO  [main] 2024-09-22 14:15:58,001 Clock.java:60 - Using java.lang.System clock to generate timestamps.
===== Using optimized driver!!! =====
INFO  [main] 2024-09-22 14:15:58,039 Cluster.java:195 - ===== Using optimized driver!!! =====
INFO  [main] 2024-09-22 14:15:58,090 NettyUtil.java:84 - Detected shaded Netty classes in the classpath; native epoll transport will not work properly, defaulting to NIO.
INFO  [main] 2024-09-22 14:15:58,496 RackAwareRoundRobinPolicy.java:128 - Using data-center name 'datacenter1' for RackAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with RackAwareRoundRobinPolicy constructor)
INFO  [main] 2024-09-22 14:15:58,496 RackAwareRoundRobinPolicy.java:136 - Using rack name 'rack1' for RackAwareRoundRobinPolicy (if this is incorrect, please provide the correct rack name with RackAwareRoundRobinPolicy constructor)
INFO  [main] 2024-09-22 14:15:58,497 Cluster.java:1810 - New Cassandra host /172.17.0.2:9042 added
Connected to cluster: , max pending requests per connection null, max connections per host 8
Datatacenter: datacenter1; Host: /172.17.0.2; Rack: rack1
INFO  [main] 2024-09-22 14:15:58,513 HostConnectionPool.java:200 - Using advanced port-based shard awareness with /172.17.0.2:9042
WARN  [cluster1-nio-worker-1] 2024-09-22 14:15:58,561 RequestHandler.java:303 - Query '[0 bound values] CREATE KEYSPACE IF NOT EXISTS "keyspace1" WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : '1'} AND durable_writes = true;' generated server side warning(s): Using Replication Factor replication_factor=1 lower than the minimum_replication_factor_warn_threshold=3 is not recommended.
Created keyspaces. Sleeping 1s for propagation.
Sleeping 2s...
Warming up WRITE with 50000 iterations...
Running WRITE with 10 threads 10 seconds
type       total ops,    op/s,    pk/s,   row/s,    mean,     med,     .95,     .99,    .999,     max,   time,   stderr, errors,  gc: #,  max ms,  sum ms,  sdv ms,      mb
total,          4884,    4884,    4884,    4884,     0.6,     0.6,     0.9,     1.1,     2.6,    10.4,    1.0,  0.00000,      0,      0,       0,       0,       0,       0
total,         21541,   16657,   16657,   16657,     0.6,     0.6,     0.8,     1.1,     3.2,     4.6,    2.0,  0.38501,      0,      0,       0,       0,       0,       0
total,         38248,   16707,   16707,   16707,     0.6,     0.6,     0.8,     1.1,     1.4,     5.5,    3.0,  0.25113,      0,      0,       0,       0,       0,       0
total,         54624,   16376,   16376,   16376,     0.6,     0.6,     0.9,     1.1,     2.6,     3.4,    4.0,  0.18494,      0,      0,       0,       0,       0,       0
total,         71272,   16648,   16648,   16648,     0.6,     0.6,     0.8,     1.0,     1.4,     2.7,    5.0,  0.14659,      0,      0,       0,       0,       0,       0
total,         88068,   16796,   16796,   16796,     0.6,     0.6,     0.8,     1.1,     3.0,     3.6,    6.0,  0.12151,      0,      0,       0,       0,       0,       0
total,        104767,   16699,   16699,   16699,     0.6,     0.6,     0.8,     1.1,     1.5,     3.5,    7.0,  0.10368,      0,      0,       0,       0,       0,       0
total,        121300,   16533,   16533,   16533,     0.6,     0.6,     0.8,     1.0,     2.1,     3.8,    8.0,  0.09036,      0,      0,       0,       0,       0,       0
total,        137876,   16576,   16576,   16576,     0.6,     0.6,     0.8,     1.1,     1.4,     3.7,    9.0,  0.08008,      0,      0,       0,       0,       0,       0
total,        154598,   16722,   16722,   16722,     0.6,     0.6,     0.8,     1.1,     1.8,     3.5,   10.0,  0.07193,      0,      0,       0,       0,       0,       0
total,        166163,   16775,   16775,   16775,     0.6,     0.6,     0.8,     1.0,     1.4,     3.3,   10.7,  0.06531,      0,      0,       0,       0,       0,       0


Results:
Op rate                   :   15,545 op/s  [WRITE: 15,545 op/s]
Partition rate            :   15,545 pk/s  [WRITE: 15,545 pk/s]
Row rate                  :   15,545 row/s [WRITE: 15,545 row/s]
Latency mean              :    0.6 ms [WRITE: 0.6 ms]
Latency median            :    0.6 ms [WRITE: 0.6 ms]
Latency 95th percentile   :    0.8 ms [WRITE: 0.8 ms]
Latency 99th percentile   :    1.1 ms [WRITE: 1.1 ms]
Latency 99.9th percentile :    1.7 ms [WRITE: 1.7 ms]
Latency max               :   10.4 ms [WRITE: 10.4 ms]
Total partitions          :    166,163 [WRITE: 166,163]
Total errors              :          0 [WRITE: 0]
Total GC count            : 0
Total GC memory           : 0.000 KiB
Total GC time             :    0.0 seconds
Avg GC time               :    NaN ms
StdDev GC time            :    0.0 ms
Total operation time      : 00:00:10

END""".replace("\n", "").replace("\r", "")


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

    def test_should_parse_op_rate_from_full_logs(self, cassandra_full_logs):
        assert CassandraStressRun._match_op_rate(cassandra_full_logs) == 15.545


class TestCassandraRunHelperMethods:

    def test_run_command_builds_correctly(self):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        assert "duration=2s" in run._run_command
        assert "-node 111.0.0.1" in run._run_command


class TestCassandraE2ERun:

    def test_single_run_accumulates_results_positive(self, cassandra_stress_output):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        run._run_command = f'echo "{cassandra_stress_output}"'

        run.run_stress_test()
        assert run.op_rate == 15.156
        assert run.latency_mean == 0.6
        assert run.latency_99th == 1.2
        assert run.latency_max == 17.3

    def test_single_run_fails_on_no_results(self, cassandra_stress_output):
        run = CassandraStressRun("thread_name", duration=2, node_ip="111.0.0.1")
        run._run_command = f'echo "EMPTY"'
        with pytest.raises(AttributeError) as err:
            run.run_stress_test()
