# Sliding Window Refresh & Partitioned Storage

These two mechanisms—**sliding window refresh** and **partitioned file storage**—can operate independently, but they are
most effective when used together. Combined, they enable efficient incremental ingestion and controlled data lifecycle
management, significantly improving performance and scalability.

---

## Sliding Window

The **sliding window** is a refresh strategy that limits data ingestion to a moving temporal range. Instead of
reprocessing the entire dataset, only the most recent slice of data is fetched and updated.

Each configuration parameter is described in detail in the [API documentation](../03_api.md/#refresh-policy-object), but
the key fields are summarized below:

| Field                        | Description                                                                                                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`                       | Must be set to `window` to enable sliding window behavior.                                                                                                                                          |
| `refreshInterval`            | Defines how often the window refresh runs. It accepts either a human interval such as `1 hour` or a cron expression.                                                                                |
| `consistencyRefreshInterval` | Optional lower-frequency schedule for a full FDA rebuild that helps recover delayed historical data. It must be greater than `refreshInterval`, otherwise `InvalidParam` error response will be got |
| `fetchSize`                  | Defines the **time range** of data to fetch on each refresh. For example, `week` fetches data from the last week.                                                                                   |
| `windowSize`                 | Specifies the total retention window (e.g., data from last month), defining which data should be preserved and which should be discarded.                                                           |

Both `postgres` and `mongodb` datasources support sliding-window refresh. In both cases, the configured time range is
applied to the source query, with the exact implementation depending on the datasource.

-   **Postgres**: the configured query is wrapped as a subquery and filtered by `timeColumn`:
    `SELECT * FROM (<query>) q WHERE <timeColumn> >= TIMESTAMP '<start>' AND <timeColumn> < NOW()`.

-   **MongoDB**: for `filter` queries, the time range is combined with the existing filter using `$and`. For
    `aggregation` queries, it is added as a `$match` stage at the beginning of the pipeline. Therefore, `timeColumn`
    must be a raw field of the source collection, available before any other pipeline stage, and contain BSON `Date`
    values.

In both datasources, the upper bound is evaluated at query execution time: Postgres uses `NOW()`, while MongoDB uses
`$expr` with `$$NOW`. This ensures that the current time is determined by the database server when each scheduled
refresh job runs.

### Bootstrap snapshot and object storage

A cached MongoDB FDA created in `strict` mode needs a schema before its first refresh. A zero-row Parquet snapshot is
therefore created synchronously when the FDA is created.

For MongoDB, the snapshot columns come from the query's declared output (`projection` or final `$project`/`$group`
stage). Their types are not known yet, so the persisted schema stores them as `null` (internally, the empty snapshot
Parquet uses `VARCHAR` columns). After the first refresh, the persisted schema is replaced with the types of the
materialized Parquet.

The zero-row snapshot is especially important for partitioned FDAs. Without it, the initial unfiltered snapshot could
create a partition based on the first document returned by MongoDB, even though that document may fall outside the
configured sliding window. The regular windowed refresh would then not revisit that partition.

---

## Partitioned Files

DuckDB supports **partitioned Parquet writes**, enabling data to be physically organized by time-based dimensions (e.g.,
year, month, day).

This approach allows the system to:

-   Avoid full dataset rewrites
-   Perform efficient incremental updates
-   Target specific partitions for deletion or refresh

For example, using a `day` partitioning strategy results in a structure like:

```
fdaId/
  year=2020/
    month=08/
      day=23/
        data_0.parquet
```

Each partition contains only the data corresponding to its time slice, stored in MinIO.

---

## Interaction & Design Considerations

While both features are independent in configuration, they are tightly coupled in practice:

-   The **sliding window** controls _what data is fetched_
-   The **partitioning strategy** controls _how that data is stored_

This separation provides flexibility, but also introduces potential misconfigurations.

### ⚠️ Common Pitfalls

-   **Partition granularity larger than refresh frequency** If partitions are coarser than the refresh interval (e.g.,
    monthly partitions with daily refresh), updates may require:

    -   deleting entire partitions
    -   re-fetching overlapping data This reduces efficiency and increases processing cost.

-   **Mismatched refresh and cleanup cadence** If the cleanup cadence implied by `refreshInterval` differs significantly
    from the intended retention behavior:

    -   shorter intervals may prematurely shrink the dataset
    -   longer intervals may allow unnecessary data accumulation

    This leads to **inconsistent data window sizes over time**.

-   **Historical gaps outside the incremental window** If a source delivers late rows, an incremental refresh may miss
    them until the next consistency rebuild.

---

## Recommended Approach

For optimal performance and predictability:

-   Align **partition granularity** with **refresh frequency** (e.g., daily refresh → daily partitions)

-   Keep the **refresh cadence** consistent with the intended cleanup cycle
-   Add a **consistency refresh** at a lower cadence when you need to recover delayed historical rows without paying the
    cost of full rebuilds on every fetch

-   Define a **clear retention window** (`windowSize`) to maintain a stable dataset size

---

By combining sliding window ingestion with partition-aware storage, the system achieves:

-   efficient incremental updates
-   minimal data duplication
-   scalable long-term data management

---

## Integration Test Scenarios

The integration suite includes real PostgreSQL-based scenarios to validate sliding-window behavior end-to-end:

-   Sliding window with daily partitioning (`fetchSize=day`, `partition=day`)
-   Sliding window with refresh intervals smaller than the partition (`refreshInterval=12 hours`, `partition=week`)
-   Manual FDA update (`PUT /fdas/:fdaId`) after creation, validating that rows outside the configured window are
    excluded after refresh
-   Validation errors for common misconfigurations:
-   `fetchSize` different from `partition`
-   partition configured without `timeColumn`
-   invalid partition type

These checks are implemented in:

-   `test/integration/suites/slidingWindows.integration.tests.js`

Equivalent scenarios for MongoDB datasources (both `filter` and `aggregation` queries) are implemented in:

-   `test/integration/suites/mongoSlidingWindows.integration.tests.js`
