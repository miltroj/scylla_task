# Even More Stress

## Introduction

This project simulates client stress on a ScyllaDB database using multiple concurrent threads. It runs a series of `cassandra-stress` commands, collects performance data, and aggregates the results for analysis.

## Prerequisites

Before running the program, you need to set up the required environment:

1. **Pull the required Docker images:**
   - ScyllaDB instance:  
     ```bash
     docker pull scylladb/scylla
     ```
   - Cassandra-stress container:  
     ```bash
     docker pull scylladb/cassandra-stress
     ```

2. **Set up Python environment:**
   - Create a Python virtual environment:
     ```bash
     python3 -m venv venv
     ```
   - Activate the virtual environment:
     - On macOS/Linux:
       ```bash
       source venv/bin/activate
       ```
     - On Windows:
       ```bash
       venv\Scripts\activate
       ```

3. **Install dependencies using Poetry:**
   - Install Poetry if you haven't already:
     ```bash
     pip install poetry
     ```
   - For regular usage, install the required packages:
     ```bash
     poetry install
     ```
   - For development, install with additional development tools:
     ```bash
     poetry install --with dev
     ```

## How to Run

To run the program, follow these steps:

1. **Start the ScyllaDB instance:**
   ```bash
   docker run --name some-scylla --hostname some-scylla -d scylladb/scylla --smp 1

1. **Get the Scylla node IP address:**

You will need the node IP to run the stress test. Run the following command to get the IP address:

```bash
docker exec -it some-scylla nodetool status
```

1. **Run the stress test:**
Use the following command to run the Python script:

```bash
even-more-stress --N_runs 2 --durations 10 10 --node_ip 172.17.0.2 --html
```

* `N_runs:` Number of concurrent cassandra-stress commands to run.
* `durations:` Duration (in seconds) for each stress test.
* `node_ip:` The IP address of the Scylla node (use the IP from the previous step).

## Testing
To run the tests for the project, you can use pytest. Make sure the development dependencies are installed:
```bash
pytest -v .
```
