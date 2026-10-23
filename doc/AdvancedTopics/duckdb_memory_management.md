# `DuckDB` memory management

## Background

Originally, `DuckDB` was configured to run entirely in memory. This allowed DuckDB to consume memory freely and, in some
cases, use a significant portion of the FDA container's available memory. This could slow down other processes and, in
extreme cases, prevent the component from completing its work.

To prevent this, we changed the configuration to use a persistent DuckDB database and introduced the following
environment variables:

-   `FDA_DUCKDB_MEMORY_LIMIT`
-   `FDA_DUCKDB_MAX_THREADS`
-   `FDA_DUCKDB_PRESERVE_INSERTION_ORDER`
-   `FDA_DUCKDB_S3_UPLOADER_MAX_FILESIZE`
-   `FDA_DUCKDB_PARTITIONED_WRITE_MAX_OPEN_FILES`

These settings allow us to control the amount of memory DuckDB is allowed to use. Together with the container's memory
limit, this gives us much tighter control over the total memory consumption of the FDA component.

However, introducing a strict DuckDB memory limit exposed a new problem: some FDA creation and refresh operations
started failing with a DuckDB **Out of Memory (OOM)** error when the configured limit was reached.

The following investigation was performed to understand where this memory was being used and determine the minimum
memory configuration required for these workloads.

---

## DuckDB out-of-core processing

DuckDB supports workloads larger than the available memory through **out-of-core processing**, primarily by spilling
intermediate data to disk.

When DuckDB reaches its memory limit, certain types of intermediate data can be moved from memory to a temporary
directory. This releases memory while allowing the operation to continue processing.

Therefore, before investigating the OOM itself, we first verified that this mechanism was working correctly.

The important distinction for this investigation is that **not all DuckDB memory allocations are necessarily
spillable**. Query intermediate data, such as `COLUMN_DATA`, can be moved to temporary storage, while memory used by
extensions may not be handled in the same way.

---

# Investigation

## Test environment

The initial tests were performed with the following configuration.

### DuckDB

```text
FDA_DUCKDB_MEMORY_LIMIT = 0.5GB
FDA_DUCKDB_MAX_THREADS = 1
FDA_DUCKDB_PRESERVE_INSERTION_ORDER = false
```

### FDA container

```yaml
mem_limit: 2g
memswap_limit: 2g
```

The relatively low DuckDB memory limit was intentional: the objective was to reproduce the OOM and understand the memory
requirements of the operation.

---

## Test FDA

The investigation uses the following FDA, which reproduced the OOM observed in production:

```json
{
    "id": "fda1",
    "query": "SELECT * FROM trantor.energyefficiency_acmeasurement WHERE timeinstant < (NOW() - INTERVAL '7 days')",
    "description": "Partitioned FDA with window refresh.",
    "timeColumn": "timeinstant",
    "refreshPolicy": {
        "type": "window",
        "params": {
            "refreshInterval": "1 hour",
            "fetchSize": "month",
            "windowSize": "6 months"
        }
    },
    "objStgConf": {
        "partition": "month"
    }
}
```

This FDA is relevant because it combines several potentially memory-intensive operations:

-   Reading a large CSV dataset.
-   Transforming the data with DuckDB.
-   Writing Parquet.
-   Writing the resulting files to S3-compatible object storage.
-   Partitioning the output by month.
-   Compressing the resulting Parquet files.

---

# Verifying out-of-core processing

The first step was to verify that DuckDB was actually using the configured temporary directory for spilling.

The temporary directory is configured through an environment variable, so the exact path may differ between
environments.

Inside the FDA container, we can inspect it with:

```bash
ls -lah /tmp/duckdb/temp
```

To monitor its usage during the operation:

```bash
while true; do
  echo "=== $(date) ==="
  du -sh /tmp/duckdb/temp
  find /tmp/duckdb/temp -maxdepth 2 -type f -exec du -h {} \;
  sleep 0.5
done
```

The first command confirms that the directory exists and has the appropriate permissions.

The directory only appears after DuckDB is initialized, which is expected.

The second command allows us to monitor the temporary files while the FDA transformation is running.

### Example

At the beginning of the transformation:

```text
=== Thu Sep  3 08:09:19 UTC 2026 ===
32M /tmp/duckdb/temp

1.8M  /tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
768K  /tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
480K  /tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
5.2M  /tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
1.1M  /tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
23M   /tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
```

The temporary storage continues to grow as the transformation progresses:

```text
=== Thu Sep  3 08:10:42 UTC 2026 ===
539M /tmp/duckdb/temp

21M  /tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
15M  /tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
22M  /tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
22M  /tmp/duckdb/temp/duckdb_temp_storage_S192K-0.tmp
22M  /tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
27M  /tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
414M /tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
```

Immediately after the OOM:

```text
=== Thu Sep  3 08:10:43 UTC 2026 ===
4.0K /tmp/duckdb/temp
```

The temporary files disappear because the failed DuckDB operation cleans them up.

### Result

This confirms that **DuckDB's out-of-core processing is working correctly**.

Temporary files are created when the transformation begins, their size increases as memory pressure increases, and they
are removed when the operation finishes or fails.

The spilling mechanism itself is therefore not the cause of the OOM.

---

# Investigating DuckDB memory consumption

Since spilling was working correctly but the operation still failed, the next step was to identify which DuckDB
components were consuming the memory.

For this purpose, a temporary diagnostic method was added to periodically log DuckDB's memory usage. For clarity,
`console.log()` was used instead of the normal FDA logger.

A timer was added when initializing the DuckDB connection:

```javascript
let loggingMemory = false;

const duckDbMemoryTimer = setInterval(async () => {
    if (loggingMemory) {
        return;
    }

    loggingMemory = true;

    try {
        await logDuckDBMemory(configConn);
    } finally {
        loggingMemory = false;
    }
}, 5000);

duckDbMemoryTimer.unref();
```

The diagnostic method shows the memory usage next to the temporary storage usage so we can see the use of the
disk-spilling the operations that allow it.

```javascript
async function logDuckDBMemory(conn) {
    console.log('===DuckDB memory===');
    console.log(
        (
            await conn.runAndReadAll(`
        SELECT
            tag,
            memory_usage_bytes / 1024 / 1024 AS memory_mb,
            temporary_storage_bytes / 1024 / 1024 AS temp_mb
        FROM duckdb_memory()
        WHERE memory_usage_bytes > 0
        ORDER BY memory_usage_bytes DESC;
  `)
        ).getRowObjects(),
    );
    console.log('======');
}
```

This allows us to correlate DuckDB's memory usage with its temporary storage usage and determine which operations are
responsible for the memory consumption.

---

## DuckDB memory tags

The `duckdb_memory()` table uses tags to identify the type of operations by memory consumption:

| Tag                   | Meaning                                                                                                   |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| `EXTENSION`           | Memory used by loaded DuckDB extensions and extension-related allocations.                                |
| `ALLOCATOR`           | Memory attributed to DuckDB's general allocator when it cannot be attributed to a more specific category. |
| `COLUMN_DATA`         | Data held in DuckDB's columnar data structures.                                                           |
| `CSV_READER`          | Memory used while reading and parsing CSV data.                                                           |
| `BASE_TABLE`          | Memory associated with persistent/base table storage.                                                     |
| `HASH_TABLE`          | Memory used by hash tables, typically for joins or hash-based aggregations.                               |
| `PARQUET_READER`      | Memory used by Parquet readers.                                                                           |
| `ORDER_BY`            | Memory used by sorting operations.                                                                        |
| `ART_INDEX`           | Memory used by ART indexes.                                                                               |
| `METADATA`            | Memory used for metadata structures.                                                                      |
| `OVERFLOW_STRINGS`    | Memory/storage for strings that do not fit normally in the relevant structures.                           |
| `IN_MEMORY_TABLE`     | Memory associated with temporary/in-memory tables.                                                        |
| `TRANSACTION`         | Transaction-related memory.                                                                               |
| `EXTERNAL_FILE_CACHE` | Cache used for externally accessed files.                                                                 |
| `WINDOW`              | Memory used by window functions.                                                                          |
| `OBJECT_CACHE`        | Object/catalog-related cache.                                                                             |

The important tags for this investigation are **`COLUMN_DATA`**, which represents the main query data and can spill to
disk, and **`EXTENSION`**, which represents memory used by DuckDB extensions.

---

# Initial memory usage

When `copyQueryToParquet()` starts, we initially see:

```text
===DuckDB memory===

{ tag: 'COLUMN_DATA', memory_mb: '430.00', temp_mb: '19.41' }
{ tag: 'CSV_READER', memory_mb: '30.52', temp_mb: '0.00' }
{ tag: 'ALLOCATOR', memory_mb: '16.22', temp_mb: '0.00' }
```

At this point, the main memory consumer is `COLUMN_DATA`.

As the operation progresses, `COLUMN_DATA` starts spilling to disk:

```text
{ tag: 'COLUMN_DATA', memory_mb: 348.25, temp_mb: 371.25 }
```

This is the expected out-of-core behaviour: part of the data remains in memory while another part is stored in temporary
files.

Later, the amount of `COLUMN_DATA` kept in memory decreases further while temporary storage continues to grow.

This confirms that the main query data is successfully being managed through DuckDB's spilling mechanism.

However, another memory consumer starts to become significant:

```text
{ tag: 'EXTENSION', memory_mb: 306, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 91.86, temp_mb: 0 }
{ tag: 'COLUMN_DATA', memory_mb: 48.25, temp_mb: 639.84 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
```

At this point, `COLUMN_DATA` only requires around 48 MB of memory while almost 640 MB has already been spilled to disk.

In contrast, `EXTENSION` is using 306 MB of memory and **0 MB of temporary storage**.

This is the first indication that the OOM is not caused by DuckDB failing to spill query data.

---

# OOM condition

The configured DuckDB settings for the failing operation were:

```text
memory_limit = 476.8 MiB
max_temp_directory_size = 9.3 GiB
preserve_insertion_order = false
temp_directory = /tmp/duckdb/temp
threads = 1
```

The operation eventually fails with:

```text
Out of Memory Error:
could not allocate block of size 76.5 MiB
(441.3 MiB/476.8 MiB used)
```

Immediately before the failure, the memory usage was approximately:

```text
EXTENSION     306 MB
ALLOCATOR      92 MB
COLUMN_DATA    48 MB
CSV_READER     31 MB
```

The important observation is that **the majority of the memory is no longer being used by the query data itself**.

The OOM occurs when DuckDB attempts to allocate another **76.5 MB** block while already using approximately **441 MB of
its 476.8 MB memory limit**.

The `COLUMN_DATA` allocation is being successfully spilled, but the `EXTENSION` allocation remains resident in memory.

---

# Isolating the source of the extension memory

At this point, the investigation focused on identifying what operation causes the `EXTENSION` memory to appear.

The FDA transformation was progressively simplified into four tests.

## 1. Basic query — no output

The first test only executes the query:

```javascript
return conn.run(`
  SELECT COUNT(*)
  FROM (${sourceQuery}) AS fda_source
`);
```

This reads the data but does not write a result or interact with S3.

The memory usage remains low:

```text
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 1.04, temp_mb: 0 }
```

No `EXTENSION` memory is present.

---

## 2. Local Parquet

The second test introduces the Parquet-writing process, but writes the file to the local filesystem:

```javascript
return conn.run(
    `COPY (
      SELECT ${cols}
      FROM (${sourceQuery}) AS fda_source
    )
    TO '/tmp/test.parquet'
    (FORMAT PARQUET);`,
);
```

This produces significantly higher `COLUMN_DATA` usage, but no `EXTENSION` memory:

```text
{ tag: 'COLUMN_DATA', memory_mb: 198.25, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 43.83, temp_mb: 0 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
```

The operation completes successfully.

This shows that Parquet generation itself is not responsible for the `EXTENSION` memory growth.

---

## 3. Non-partitioned S3 Parquet

The third test changes only the destination from the local filesystem to S3:

```javascript
return conn.run(
    `COPY (
      SELECT ${cols}
      FROM (${sourceQuery}) AS fda_source
    )
    TO 's3://${resultPath}'
    (FORMAT PARQUET);`,
);
```

Now `EXTENSION` memory appears:

```text
{ tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 }
```

Later it increases:

```text
{ tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 }
```

This establishes that the additional memory usage is associated with the S3-writing path, which is provided by the
`httpfs` extension.

However, this operation still completes successfully under the 0.5 GB memory limit.

---

## 4. Partitioned S3 Parquet

Finally, the complete production query is executed:

```javascript
return conn.run(
    `COPY (
      SELECT ${cols}
      FROM (${sourceQuery}) AS fda_source
    )
    TO 's3://${resultPath}'
    (FORMAT PARQUET ${partitionBy} ${compressionString});`,
);
```

This adds the partitioning and compression configuration used by the FDA.

This is the operation that reproduces the production OOM.

The logs show that `EXTENSION` memory grows significantly:

```text
{ tag: 'EXTENSION', memory_mb: 306, temp_mb: 0 }
```

while `COLUMN_DATA` is successfully being spilled:

```text
{ tag: 'COLUMN_DATA', memory_mb: 48.25, temp_mb: 639.84 }
```

The simplified tests therefore allow us to isolate the behaviour:

| Test                   | S3  | Partitioning | `EXTENSION` memory | Result         |
| ---------------------- | --- | ------------ | ------------------ | -------------- |
| Basic query            | No  | No           | None               | Success        |
| Local Parquet          | No  | No           | None               | Success        |
| S3 Parquet             | Yes | No           | ~76–153 MB         | Success        |
| Partitioned S3 Parquet | Yes | Yes          | ~306 MB            | OOM at ~500 MB |

This strongly indicates that the memory pressure is specifically related to **partitioned S3 writes**.

---

# DuckDB `httpfs` memory investigation

To understand the source of the `EXTENSION` memory, the DuckDB `httpfs` S3 upload implementation was investigated.

The S3 upload implementation uses `S3UploadSession`, which manages multipart uploads and allocates upload buffers
through DuckDB's buffer manager.

These allocations are accounted for under the:

```text
EXTENSION
```

memory tag.

Unlike query data such as `COLUMN_DATA`, these buffers are not necessarily spillable to disk.

The uploader also uses an adaptive multipart upload strategy: as more upload parts are reserved, the target part size
can increase.

This means that S3 upload buffers can represent a significant amount of resident memory during large or highly
partitioned writes.

This explains the behaviour observed in the tests:

-   Local Parquet writes do not require `httpfs` and do not show `EXTENSION` memory.
-   S3 Parquet writes activate `httpfs` and introduce `EXTENSION` memory.
-   Partitioned S3 writes require more upload activity and result in substantially higher `EXTENSION` memory usage.
-   This memory is not compensated for by the normal spilling mechanism because it is not the same type of memory as
    spillable query data.

---

# `httpfs` configuration tests

A first round of tests tried these settings to reduce the memory of the extension:

```text
partitioned_write_max_open_files=1
s3_uploader_thread_limit=1
s3_uploader_max_filesize='80GB'
```

None of them prevented the `EXTENSION` memory from growing enough to reproduce the problem, which led to the conclusion
that no `httpfs` setting could limit it. That conclusion was wrong.

**The part size depends on `s3_uploader_max_filesize`.** Each file being uploaded keeps one multipart part in memory,
and the part size is `s3_uploader_max_filesize / s3_uploader_max_parts_per_file`. With the defaults this is
`800GB / 10000 = 80 MB`, the `76.5 MiB` block that appears in the errors. `80GB` still leaves 8 MB parts. The minimum
DuckDB allows, `50GB`, gives 5 MB parts, the minimum S3 part size. The memory of a partitioned export is therefore:

```text
EXTENSION ≈ min(partitions written, partitioned_write_max_open_files) × s3_uploader_max_filesize / 10000
```

**Both settings are per connection.** A plain `SET` only changes them for the connection that runs it (scope `LOCAL` in
`duckdb_settings()`). Set on any other connection than the one that runs the `COPY`, they have no effect, which may be
why `partitioned_write_max_open_files=1` made no difference in the first round. FDA applies them with `SET GLOBAL` when
it creates the DuckDB instance, so every connection uses them.

Measured with DuckDB 1.5.6 inside the FDA container, writing to MinIO: 3 million rows over ~35 days, partitioned by day,
`threads=2`, `preserve_insertion_order=false`, peak sampled from `duckdb_memory()` every 50 ms.

| Settings                                        | memory_limit | Result | Peak     | `EXTENSION` |
| ----------------------------------------------- | ------------ | ------ | -------- | ----------- |
| defaults                                        | 1 GB         | OOM    | ~1000 MB | 963 MB      |
| `partitioned_write_max_open_files=8`            | 1 GB         | ok     | 694 MB   | 642 MB      |
| `s3_uploader_max_filesize='80GB'`               | 256 MB       | OOM    | 256 MB   | 228 MB      |
| `s3_uploader_max_filesize='50GB'`               | 256 MB       | ok     | 240 MB   | 182 MB      |
| `'50GB'` + `partitioned_write_max_open_files=8` | **128 MB**   | **ok** | 107 MB   | 39 MB       |

With both settings, an export that failed with 1 GB completes with 128 MB. FDA exposes them as
`FDA_DUCKDB_S3_UPLOADER_MAX_FILESIZE` (default `50GB`) and `FDA_DUCKDB_PARTITIONED_WRITE_MAX_OPEN_FILES` (default `8`).

Two side effects:

-   `50GB` is also the largest Parquet file DuckDB can upload, which is far above the size of an FDA file.
-   With more partitions than open files allowed, DuckDB closes partition files and opens new ones. When the rows are
    not ordered by time, a partition directory can end up with several files (`data_0.parquet`, `data_1.parquet`, ...).
    Queries and partition pruning read all of them, and a refresh replaces every file of the partitions it rewrites.

---

# Increasing the DuckDB memory limit

The final test was performed by increasing:

```text
FDA_DUCKDB_MEMORY_LIMIT=1GB
```

The resulting DuckDB setting was:

```text
memory_limit = 953.6 MiB
```

The operation was then allowed to run without the artificially restrictive 0.5 GB limit.

The memory usage reached significantly higher values:

```text
{ tag: 'COLUMN_DATA', memory_mb: 647, temp_mb: 16.16 }
{ tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 18.84, temp_mb: 0 }
```

Later, the memory distribution changed:

```text
{ tag: 'EXTENSION', memory_mb: 229.5, temp_mb: 0 }
{ tag: 'COLUMN_DATA', memory_mb: 223.5, temp_mb: 52.53 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 16.22, temp_mb: 0 }
```

The maximum observed usage was approximately:

```text
{ tag: 'COLUMN_DATA', memory_mb: 519.5, temp_mb: 235 }
{ tag: 'EXTENSION', memory_mb: 382.5, temp_mb: 0 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 21.30, temp_mb: 0 }
```

After this peak, the `EXTENSION` memory was released:

```text
{ tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 }
{ tag: 'CSV_READER', memory_mb: 30.52, temp_mb: 0 }
{ tag: 'ALLOCATOR', memory_mb: 1, temp_mb: 0 }
```

The FDA creation then completed successfully.

---

# Automated window-memory regression scenarios

The performance suite includes `Window memory:` scenarios in
[`fdaWindowMemory.performance.tests.js`](../../test/performance/suites/fdaWindowMemory.performance.tests.js). They use
real PostgreSQL, MinIO and MongoDB containers and run FDA as child processes, with both combined and separated roles.

The daily scenario follows the traffic FDA configuration:

-   Private visibility and `Fiware-ServicePath: /fdas`.
-   `cached: true`, with explicit source-column aliases including `fechahora AS fecha_medicion`.
-   A monthly initial window, daily partitions and daily incremental fetches.
-   The daily cron expression `0 16 * * *`.
-   Two forced recurring refreshes after creation. Each changes the recent source rows and checks both the total cached
    row count and the updated values, so merely reaching a terminal job state is not sufficient.

The fixture spreads rows across 32 days, ensuring that the initial window writes at least 28 populated daily partitions
and excludes older rows. Each description contains 256 characters derived from distinct MD5 values, avoiding the
unrealistically small Parquet files produced by repeated constant strings. This is a synthetic stress workload, not a
claim about the actual size or compressibility of production traffic descriptions.

A monthly-partition control uses the same fixture and initial window. Its `fetchSize` is also `month`, because the API
requires fetch size and partition size to match. The control verifies creation and cached contents; the daily scenario
additionally verifies recurring refreshes.

The number of source rows is controlled by `PERFORMANCE_TABLE_ROWS`. The performance harness defaults explicitly to
`1GB`, one DuckDB thread and `preserve_insertion_order=false`, while allowing overrides through the corresponding
`FDA_DUCKDB_*` environment variables. It also uses a unique, interpolated temporary directory per process.

## Running the scenarios

To run the fixed 3-million-row regression matrix with **both 2 GB and 3 GB**, use:

```bash
npm run test:performance:memory
```

This runs both combined and separated roles, starting fresh FDA processes and infrastructure for each limit. At **2
GB**, the monthly control must complete and the daily scenario must fail during creation with an explicit
`Out of Memory Error`. At **3 GB**, both must complete, including two incremental refreshes for the daily scenario. The
matrix passes only if all these expectations hold; a timeout, an unrelated error, or unexpected success at 2 GB fails
the matrix. A future fix that eliminates the OOM will therefore require updating that expectation. The command fixes the
source row count at 3 million and overrides any externally supplied DuckDB memory limit with the matrix values. Thread
count and insertion-order settings remain overridable as described above. FDA still runs as a child process on the host,
not inside a memory-limited container. The complete matrix was verified on 2026-10-19: all eight cases passed their
expected outcomes in approximately six minutes, including the expected OOM at 2 GB in both runtime modes.

Node.js 24+, installed project dependencies and a working Docker runtime are required. Run the scenarios alone to avoid
confusing their memory consumption with concurrent load tests:

```bash
PERFORMANCE_TABLE_ROWS=100000 FDA_DUCKDB_MEMORY_LIMIT=1GB \
  npm run test:performance -- --testNamePattern='Window memory:'
```

To exercise the combined-role runtime under greater memory pressure:

```bash
PERFORMANCE_TABLE_ROWS=3000000 FDA_DUCKDB_MEMORY_LIMIT=2GB \
  npm run test:performance -- --testNamePattern='combined roles.*Window memory:'
```

Repeat with `FDA_DUCKDB_MEMORY_LIMIT=0.5GB` or `1GB` to compare the same workload at lower limits. Use
`separated roles.*Window memory:` to select the separated runtime. For production comparisons, explicitly set
`FDA_DUCKDB_MAX_THREADS` and `FDA_DUCKDB_PRESERVE_INSERTION_ORDER` to the deployed values.

Outside the explicit 2 GB expected-OOM matrix case, an OOM is a **test failure**. The performance poller reports the
FDA's error immediately instead of waiting for a timeout or treating advanced progress as success. Fixture row counts,
populated dates and the configured memory limit are logged before creation.

## Reproduction observed on 2026-10-19

On the current `1.7.0-next` code, using one thread and insertion-order preservation disabled:

| Source rows | DuckDB limit | Runtime                | Monthly control | Daily partitions                  |
| ----------- | ------------ | ---------------------- | --------------- | --------------------------------- |
| 100,000     | 1 GB         | Combined and separated | Passed          | Creation and two refreshes passed |
| 1,000,000   | 0.5 GB       | Combined               | Passed          | Creation and two refreshes passed |
| 1,000,000   | 1 GB         | Combined               | Passed          | Creation and two refreshes passed |
| 3,000,000   | 0.5 GB       | Combined               | Passed          | OOM during initial creation       |
| 3,000,000   | 1 GB         | Combined               | Passed          | OOM during initial creation       |
| 3,000,000   | 2 GB         | Combined               | Passed          | OOM during initial creation       |
| 3,000,000   | 3 GB         | Combined               | Passed          | Creation and two refreshes passed |

With the 3 GB setting, DuckDB reported an effective limit of 2.7 GiB. The monthly control completed in approximately 26
seconds and the daily scenario, including creation, verification and two refreshes, in approximately 45 seconds. This
successful run does not establish the minimum required limit or guarantee operation inside a 4 GB FDA container.

The failing daily scenarios reported, respectively:

```text
Out of Memory Error: failed to allocate data of size 20.2 MiB (461.6 MiB/476.8 MiB used)
Out of Memory Error: could not allocate block of size 76.5 MiB (908.6 MiB/953.6 MiB used)
Out of Memory Error: failed to allocate data of size 32.0 MiB (1.8 GiB/1.8 GiB used)
```

The 3-million-row fixture placed 2,812,500 rows inside the monthly window, across 30 populated dates. This reproduces a
memory-limit failure for daily partitioned writes while the monthly control completes, including the production-like
effective 1.8 GiB limit. The 1 GB run also reproduces the 76.5 MiB allocation size. It does not establish that these
allocations have the same memory tag as the production allocation. These scenarios do not sample `duckdb_memory()` or
impose a total FDA-process container memory limit; the child processes run on the host, so increase workload sizes
cautiously on shared machines. The performance suite remains separate from the unit/integration CI jobs.

---

# Conclusions

The investigation leads to the following conclusions:

1. **DuckDB's out-of-core processing is working correctly.** `COLUMN_DATA` is successfully spilled to the configured
   temporary directory when memory pressure increases.

2. **The OOM is not caused by the temporary storage limit.** The temporary directory has enough capacity and reaches
   hundreds of MB during the operation without causing an error.

3. **The problematic memory is primarily associated with `EXTENSION` allocations.** These allocations remain in memory
   while other query data is being spilled.

4. **The `EXTENSION` memory is introduced by the S3-writing path.** Local Parquet generation does not produce this
   memory category, while S3 writes do.

5. **Partitioned S3 writes significantly increase the memory requirement.** Non-partitioned S3 writes completed
   successfully with substantially lower `EXTENSION` memory usage, while partitioned writes reached approximately 306 MB
   with a 0.5 GB DuckDB limit.

6. **The `httpfs` S3 uploader uses resident multipart-upload buffers.** These buffers are accounted for under
   `EXTENSION` and are not necessarily spillable in the same way as `COLUMN_DATA`.

7. **The `httpfs` memory can be limited with two settings.** `s3_uploader_max_filesize` sets the size of each upload
   part and `partitioned_write_max_open_files` how many partition files upload at the same time. With `50GB` and `8`,
   the partitioned export completes with a 128 MB limit.

8. **A higher DuckDB memory limit is not a reliable fix.** It was enough for the test FDA above (382.5 MB of `EXTENSION`
   with 1 GB), but with the default settings `EXTENSION` grows with the number of partitions: an export with 35 daily
   partitions failed with 1 GB, with 963 MB of `EXTENSION`.

Therefore, the current evidence indicates that the production OOM is caused by the memory requirements of the
**partitioned S3 upload performed by `httpfs`**, rather than a failure of DuckDB's out-of-core processing.

The important consequence for the FDA configuration is that the DuckDB memory limit must leave enough headroom for these
non-spillable extension allocations, which `FDA_DUCKDB_S3_UPLOADER_MAX_FILESIZE` and
`FDA_DUCKDB_PARTITIONED_WRITE_MAX_OPEN_FILES` keep small.

# Observability of DuckDB memory

Every Parquet export logs the DuckDB memory before it starts and the peak sampled while it runs, at `INFO` when it
succeeds and at `WARN` when it fails (see the operational guide). The Fetcher heartbeat, `GET /health` and
`GET /metrics` report the DuckDB memory state of the process between exports. Use them to decide the memory limit before
changing it:

-   The peak logs show which allocation tags were holding memory when an export failed. A large `EXTENSION` value is the
    non-spillable `httpfs` memory described above, so the limit must leave room for it.
-   `fda_duckdb_memory_limit_bytes` and `fda_duckdb_memory_used_bytes` show the effective `memory_limit` and current
    usage. Outside exports the usage is usually `0`, because DuckDB releases memory when an export ends.
-   `fda_duckdb_memory_usage_bytes{tag=...}` and `fda_duckdb_temp_storage_bytes` give the same breakdown and the bytes
    spilled to `FDA_DUCKDB_TEMP_DIR` at scrape time.
