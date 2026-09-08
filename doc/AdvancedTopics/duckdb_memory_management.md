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

Several `httpfs`-related settings were tested to determine whether the extension's memory usage could be reduced:

```text
partitioned_write_max_open_files=1
s3_uploader_thread_limit=1
s3_uploader_max_filesize='80GB'
```

None of these changes prevented the `EXTENSION` memory from growing enough to reproduce the problem.

At this point, there does not appear to be a configuration option that provides a direct memory limit for the `httpfs`
S3 upload buffers.

Therefore, reducing the DuckDB `memory_limit` alone cannot guarantee that this workload will succeed: the memory limit
applies to DuckDB's overall managed memory, but some extension allocations still need to remain resident.

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

7. **The tested `httpfs` configuration options do not provide a sufficient solution.**
   `partitioned_write_max_open_files`, `s3_uploader_thread_limit` and `s3_uploader_max_filesize` did not prevent the
   high extension memory usage.

8. **A higher DuckDB memory limit allows the operation to complete.** With a 1 GB limit, the workload reached
   approximately **382.5 MB of `EXTENSION` memory** and completed successfully.

Therefore, the current evidence indicates that the production OOM is caused by the memory requirements of the
**partitioned S3 upload performed by `httpfs`**, rather than a failure of DuckDB's out-of-core processing.

The important consequence for the FDA configuration is that the DuckDB memory limit must leave enough headroom for these
non-spillable extension allocations in addition to the memory required by the query itself.
