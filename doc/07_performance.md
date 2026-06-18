# Performance

FDA supports multiple use cases, configurations, and execution modes that can have a significant impact on performance
in demanding scenarios. To monitor and validate these cases, we maintain a dedicated performance test suite.

The suite is divided into two categories:

-   **Benchmark tests**, which measure the execution time of critical FDA operations and queries.
-   **Load tests**, which execute concurrent requests with optional ramp-up periods to evaluate system behavior under
    load.

Like the [integration tests](/doc/06_testing.md), performance tests run against a real environment using Docker
containers and HTTP requests. To simulate realistic workloads, the suite generates a large PostgreSQL table and uses it
as the source dataset for all tests. The size of this table can be configured through the test parameters.

## Test Coverage

### FDA Creation

Measures the time required to create FDAs using different configurations:

-   Basic FDA creation
-   Compressed FDA creation
-   Partitioned FDA creation
-   Fresh FDA creation

The same PostgreSQL table and base query are used across all creation tests to ensure consistent comparisons.

### Query Performance

Measures query execution times against the FDAs created during the benchmark phase:

-   Basic query
-   Compressed FDA query
-   Partitioned FDA query
-   Date-filtered query against a partitioned FDA
-   Fresh FDA query

These tests cover both generic and FDA-specific access patterns to evaluate the effectiveness of different storage
configurations.

### Load and Stress Testing

Measures application performance under concurrent workload by creating multiple FDAs simultaneously.

Load tests support configurable concurrency levels and optional ramp-up periods, allowing simulation of both burst
traffic and progressively increasing workloads.

## Running the Tests

Execute the complete performance test suite with:

```bash
npm run test:performance
```

### Parameters

The following parameters can be used to customize the test execution:

| Parameter            | Description                                                                                                                                  | Default   |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| performanceTableRows | Number of rows generated in the PostgreSQL table used by the tests.                                                                          | 1_000_000 |
| maxTimeOutMs         | Timeout for each individual test (ms).                                                                                                       | 300_000   |
| fdaLoadTestCount     | Number of FDAs created concurrently during load tests.                                                                                       | 5         |
| fdaLoadRampUpMs      | Ramp-up period (ms). If `0`, all FDAs are submitted simultaneously. Otherwise, submissions are distributed evenly across the ramp-up window. | 0         |

Example:

```bash
npm run test:performance -- \
  --performanceTableRows=100000 \
  --maxTimeOutMs=500000 \
  --fdaLoadTestCount=50 \
  --fdaLoadRampUpMs=5000
```

## Test Results

Each execution mode produces three result tables.

### 1. Test Configuration

Provides context for the measurements by displaying the configuration used during the run.

| Measure                 | Description                                                                           |
| ----------------------- | ------------------------------------------------------------------------------------- |
| Rows in table           | Number of rows in the generated PostgreSQL table. Defined by `performanceTableRows`.  |
| PostgreSQL table size   | Size of the generated PostgreSQL table.                                               |
| Concurrent FDAs created | Number of FDAs created concurrently during load tests. Defined by `fdaLoadTestCount`. |
| Max timeout (ms)        | Timeout applied to individual tests. Defined by `maxTimeOutMs`.                       |
| Load ramp-up (ms)       | Ramp-up period used during load tests. Defined by `fdaLoadRampUpMs`.                  |

### 2. Performance Measurements

Contains the primary benchmark results collected during the test run.

| Measure                      | Description                                                                                          |
| ---------------------------- | ---------------------------------------------------------------------------------------------------- |
| Basic FDA creation           | Total end-to-end time required to create a standard FDA.                                             |
| Fetch time                   | Time spent waiting for the FDA to reach the fetching/transforming phase.                             |
| Parquet conversion           | Time between fetch completion and upload start, representing Parquet generation.                     |
| Compression time             | Time spent performing data compression.                                                              |
| Compressed FDA creation      | Total end-to-end time required to create a compressed FDA.                                           |
| Partition time               | Time spent performing data partitioning.                                                             |
| Partitioned FDA creation     | Total end-to-end time required to create a partitioned FDA.                                          |
| Fresh FDA creation           | Total end-to-end time required to create a fresh FDA with caching disabled.                          |
| Basic query                  | Execution time of a standard DA query with ND-JSON output format.                                    |
| Basic query (JSON)           | Execution time of a standard DA query with JSON output format.                                       |
| Basic query (CSV)            | Execution time of a standard DA query with CSV output format.                                        |
| Compressed query             | Execution time of a DA query against a compressed FDA.                                               |
| Partitioned query            | Execution time of a DA query against a partitioned FDA.                                              |
| Partitioned date-based query | Execution time of a date-filtered query against a partitioned FDA.                                   |
| Fresh query                  | Execution time of a query against a fresh FDA.                                                       |
| Creation ramp up             | Measured time between the first and last FDA submission request.                                     |
| Creation load completion     | Time from the start of completion polling until all FDAs have finished processing.                   |
| Query ramp up                | Measured time between the first and the last concurrent query requests being sent.                   |
| Query load completion        | Measured time from the start of the concurrent query batch until all query responses have completed. |

### 3. Load Test Statistics

Provides detailed latency metrics for the concurrent FDA creation tests.

| Measure      | Description                                                             |
| ------------ | ----------------------------------------------------------------------- |
| Min (ms)     | Fastest FDA completion time.                                            |
| P50 (ms)     | Median FDA completion time.                                             |
| P90 (ms)     | 90th percentile FDA completion time.                                    |
| P95 (ms)     | 95th percentile FDA completion time.                                    |
| P99 (ms)     | 99th percentile FDA completion time.                                    |
| Max (ms)     | Slowest FDA completion time.                                            |
| Avg (ms)     | Average FDA completion time across all completed FDAs.                  |
| Total (ms)   | Total duration of the completion phase.                                 |
| Ramp-up (ms) | Actual measured time between the first and last FDA submission request. |

---

## 🧭 Navigation

-   [⬅️ Previous: Testing](/doc/06_testing.md)
-   [🏠 Main index](../README.md#documentation)
