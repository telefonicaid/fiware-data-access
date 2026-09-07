# `DuckDb` memory management

With the change from having `DuckDb` instance run in memory to using a persistent database and the introduction of the
[`DuckDb` environment variables](../04_config_operational_guide.md#duckdb) `FDA_DUCKDB_MEMORY_LIMIT`,
`FDA_DUCKDB_MAX_THREADS` and `FDA_DUCKDB_PRESERVE_INSERTION_ORDER` we solved an error where `DuckDb` used memory freely
and hijacked the containers total memory, slowing other process and, in some cases, even stopping the full component.
Thanks to those variables we can control the maximum amount of memory we want `DuckDb` to use, and together with the
containers maximum memory restriction we can have a strict control over the total memory of the component.

But this memory restriction brings a new problem: we started seeing a new `DuckDb` OOM (out of memory error) with some
`FDA` creations because we were reaching the max amount of memory configured. To search best configuration for our
`DuckDb` component we have made an analysis of the memory consumption of a memory intense operation that throws an OOM.

## `DuckDb` out-of-core Processing

`DuckDb` supports larger-than-memory workloads mainly through the spilling to disk functionality. This consists of a
temporary directory where `DuckDb` stores temporary data to free memory.

Even with this mechanisms we were seeing the OOM error so we decided to check everything was working as intented:

## Testing

### Environment info

`DuckDB`:

-   FDA_DUCKDB_MEMORY_LIMIT = 0.5GB
-   FDA_DUCKDB_MAX_THREADS = 1
-   FDA_DUCKDB_PRESERVE_INSERTION_ORDER = false

`FDA` Container:

-   mem_limit: 2g
-   memswap_limit: 2g

### FDA

for this test we are gonna use the following `FDA` htat was throwing the _OOM_ error in production:

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

### Spilling to disk

The first thing is cheking the spilling to disk functionality is working as intended. For that we need to check the
configured temporary directory. Inside the docker container we can use the following commands to check the existence of
the file:

_Disclaimer:_ the temp directory is defined by a environment variable so the path used in the commands might change.

```bash
ls -lah /tmp/duckdb/temp

while true; do echo "=== $(date) ==="; du -sh /tmp/duckdb/temp; find /tmp/duckdb/temp -maxdepth 2 -type f -exec du -h {} \;; sleep 0.5; done
```

With the first command we can check the temp directory exists and has the appropriate permissions. We only find the
directory after `DuckDb` initialitation (as expected). After that we can use the second command to see the size of the
directory and a branched view of the files underneath:

```
=== Thu Sep  3 08:09:18 UTC 2026 ===
4.0K	/tmp/duckdb/temp
=== Thu Sep  3 08:09:18 UTC 2026 ===
4.0K	/tmp/duckdb/temp
=== Thu Sep  3 08:09:19 UTC 2026 ===         <-- Start of the DuckDb operation (csv to parquet conversion)
32M	/tmp/duckdb/temp
1.8M	/tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
768K	/tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
480K	/tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
5.2M	/tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
1.1M	/tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
23M	/tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
=== Thu Sep  3 08:09:19 UTC 2026 ===
82M	/tmp/duckdb/temp
3.5M	/tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
1.2M	/tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
1.4M	/tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
2.1M	/tmp/duckdb/temp/duckdb_temp_storage_S192K-0.tmp
11M	/tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
224K	/tmp/duckdb/temp/duckdb_temp_storage_S224K-0.tmp
4.7M	/tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
58M	/tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
.
.
.
=== Thu Sep  3 08:10:42 UTC 2026 ===
539M	/tmp/duckdb/temp
21M	/tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
15M	/tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S192K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
27M	/tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
414M	/tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
=== Thu Sep  3 08:10:43 UTC 2026 ===
539M	/tmp/duckdb/temp
21M	/tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp
15M	/tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S192K-0.tmp
22M	/tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp
27M	/tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp
414M	/tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp
=== Thu Sep  3 08:10:43 UTC 2026 ===            <--  OOM error
4.0K	/tmp/duckdb/temp
=== Thu Sep  3 08:10:44 UTC 2026 ===
4.0K	/tmp/duckdb/temp
```

We can see how `DuckDb` starts using the temp directory configured in the env var to upload temporary and intermediate
data. This usage starts when the `FDA` status changes from `fetching` to `transforming`, as expected because thats when
we start using `DuckDb` to convert the uploaded _csv_ file to a _parquet_ file.

### Memory consumption

Now we know the spilling to disk mechanism is working as intended, but we are still getting the **OOM** error, so we are
gonna check the memory usage of `DuckDb`. For that purpose we are gonna insert a method that logs `DuckDb` memory
consumption broken down by operation, so we known what action is more memory intensive. For the shake of clarity in the
analysis we are using `console.log()` method instead of the proper logger component:

```javascript
(...)
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
(...)
```

```javascript
async function logDuckDBMemory(conn) {
    const reader = await conn.runAndReadAll(`
    SELECT
      tag,
      memory_usage_bytes,
      temporary_storage_bytes
    FROM duckdb_memory()
    ORDER BY memory_usage_bytes DESC
  `);

    const rows = reader.getRowObjects();
    console.log('===DuckDB memory===');
    for (const row of rows) {
        console.log({
            tag: String(row.tag),
            memory_mb: (Number(row.memory_usage_bytes) / 1024 / 1024).toFixed(2),
            temp_mb: (Number(row.temporary_storage_bytes) / 1024 / 1024).toFixed(2),
        });
    }

    const tempReader = await conn.runAndReadAll(`
    SELECT
      path,
      size
    FROM duckdb_temporary_files()
    ORDER BY size DESC
  `);

    const tempRows = tempReader.getRowObjects();
    console.log('===DuckDB temp file===');
    for (const row of tempRows) {
        console.log({
            path: String(row.path),
            size_mb: Number(row.size) / 1024 / 1024,
        });
    }
    console.log('==========');
}
```

We introduce the method `logDuckDBMemory(conn)` when initialising the `DuckDb` connection to log in a interval, like we
do with the _heartbeat_ functionality. In the method we query two `DuckDb` internal tables, `duckdb_memory()` and
`duckdb_temporary_files()`. The first one stores memory usage and temporary storeage by operation ("tags"). The second
one simply stores the path of each temporary file and it's size. We are gonna log both so we can see at the same time
which operations are being executed and how they are using the spill to disk functionality.

We start seeing `DuckDb` memory and temp storage usage when starting the _copyQueryToParquet()_ method is called (again,
as expected):

```
.
.
.
time=2026-09-01T12:55:36.576Z | lvl=DEBUG | corr=job-6a96cb2834dbdc56c9327101 | trans=d1e27a11-d684-4fe3-ba6d-a804a4b8fb93 | op=refresh-fda | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | resultPath=postgres/tmp/public/fda1.parquet | msg=[DEBUG]: copyQueryToParquet
===DuckDB memory===
{ tag: 'COLUMN_DATA', memory_mb: '430.00', temp_mb: '19.41' }
{ tag: 'CSV_READER', memory_mb: '30.52', temp_mb: '0.00' }
{ tag: 'ALLOCATOR', memory_mb: '16.22', temp_mb: '0.00' }
{ tag: 'BASE_TABLE', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'HASH_TABLE', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'PARQUET_READER', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'ORDER_BY', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'ART_INDEX', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'METADATA', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'OVERFLOW_STRINGS', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'IN_MEMORY_TABLE', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'EXTENSION', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'TRANSACTION', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'EXTERNAL_FILE_CACHE', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'WINDOW', memory_mb: '0.00', temp_mb: '0.00' }
{ tag: 'OBJECT_CACHE', memory_mb: '0.00', temp_mb: '0.00' }
===DuckDB temp file===
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_DEFAULT-0.tmp',
  size_mb: 13.75
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S32K-0.tmp',
  size_mb: 3.53125
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S160K-0.tmp',
  size_mb: 0.78125
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S128K-0.tmp',
  size_mb: 0.75
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S64K-0.tmp',
  size_mb: 0.6875
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S192K-0.tmp',
  size_mb: 0.5625
}
{
  path: '/tmp/duckdb/temp/duckdb_temp_storage_S96K-0.tmp',
  size_mb: 0.1875
}
.
.
.
```

The temporary file information is the same as we observed before so we are gonna ignore it now that we know its working
as intended. In the memory usage table we can see all the operations/tags mentioned before. A brief explanation of each
one:

| Tag                   | Meaning                                                                                                   |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| `EXTENSION`           | Memory used by loaded DuckDB extensions / extension-related allocations.                                  |
| `ALLOCATOR`           | Memory attributed to DuckDB's general allocator (allocations not attributed to a more specific category). |
| `COLUMN_DATA`         | Data held in DuckDB's columnar data structures.                                                           |
| `CSV_READER`          | Memory used while reading/parsing CSV data.                                                               |
| `BASE_TABLE`          | Memory associated with persistent/base table storage.                                                     |
| `HASH_TABLE`          | Memory used by hash tables, typically joins or hash-based aggregations.                                   |
| `PARQUET_READER`      | Memory used by Parquet readers.                                                                           |
| `ORDER_BY`            | Memory used by sorting operations.                                                                        |
| `ART_INDEX`           | Memory used by ART indexes.                                                                               |
| `METADATA`            | Memory used for metadata structures.                                                                      |
| `OVERFLOW_STRINGS`    | Memory/storage for strings that don't fit normally in the relevant structures.                            |
| `IN_MEMORY_TABLE`     | Memory associated with temporary/in-memory tables.                                                        |
| `TRANSACTION`         | Transaction-related memory.                                                                               |
| `EXTERNAL_FILE_CACHE` | Cache for externally accessed files.                                                                      |
| `WINDOW`              | Memory used by window functions.                                                                          |
| `OBJECT_CACHE`        | Object/catalog-related cache.                                                                             |

With this information in mind we are gonna see the moments of maximun memory usage and the moment before the _OOM_
error:

```
time=2026-09-03T17:20:37.886Z | lvl=INFO | corr=7bf17ce0-2a4d-4371-ae3f-79ea8f59d0a6 | trans=c7e94c76-213f-4712-b01f-9920d6f84a6e | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=max_temp_directory_size | value=9.3 GiB | msg=DuckDB setting
time=2026-09-03T17:20:37.886Z | lvl=INFO | corr=7bf17ce0-2a4d-4371-ae3f-79ea8f59d0a6 | trans=c7e94c76-213f-4712-b01f-9920d6f84a6e | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=memory_limit | value=476.8 MiB | msg=DuckDB setting
time=2026-09-03T17:20:37.886Z | lvl=INFO | corr=7bf17ce0-2a4d-4371-ae3f-79ea8f59d0a6 | trans=c7e94c76-213f-4712-b01f-9920d6f84a6e | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=preserve_insertion_order | value=false | msg=DuckDB setting
time=2026-09-03T17:20:37.887Z | lvl=INFO | corr=7bf17ce0-2a4d-4371-ae3f-79ea8f59d0a6 | trans=c7e94c76-213f-4712-b01f-9920d6f84a6e | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=temp_directory | value=/tmp/duckdb/temp | msg=DuckDB setting
time=2026-09-03T17:20:37.887Z | lvl=INFO | corr=7bf17ce0-2a4d-4371-ae3f-79ea8f59d0a6 | trans=c7e94c76-213f-4712-b01f-9920d6f84a6e | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=threads | value=1 | msg=DuckDB setting

.
.
.

===DuckDB memory===    <-- Start memory log
[
  { tag: 'COLUMN_DATA', memory_mb: 113.25, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 16.22021484375, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 348.25, temp_mb: 371.25 },
  { tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 21.3623046875, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 243.75, temp_mb: 290.78125 },
  { tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 61.0390625, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 18.8388671875, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 201.75, temp_mb: 585.28125 },
  { tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 91.69651794433594, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 }
]
======

.
.
.
[
  { tag: 'EXTENSION', memory_mb: 306, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 91.86058902740479, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 48.25, temp_mb: 639.84375 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 }
]

.
.
.

[
  { tag: 'EXTENSION', memory_mb: 306, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 91.8586254119873, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 48.25, temp_mb: 656 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 }
]
======
time=2026-09-03T17:22:42.088Z | lvl=DEBUG | corr=job-6a99ac67745ba3604f987fe1 | trans=054a5d57-8e03-4a0f-aeec-20358910f5fa | op=refresh-fda | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | msg=MongoDB connection to db fiware-data-access
time=2026-09-03T17:22:42.094Z | lvl=DEBUG | corr=job-6a99ac67745ba3604f987fe1 | trans=054a5d57-8e03-4a0f-aeec-20358910f5fa | op=refresh-fda | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | bucket=postgres | prefix=tmp/public/fda1.parquet/ | msg=[DEBUG]: listObjects
time=2026-09-03T17:22:42.105Z | lvl=ERROR | corr=job-6a99ac67745ba3604f987fe1 | trans=054a5d57-8e03-4a0f-aeec-20358910f5fa | op=refresh-fda | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | err=FDAError: Out of Memory Error: could not allocate block of size 76.5 MiB (441.3 MiB/476.8 MiB used)

Possible solutions:
* Reducing the number of threads (SET threads=X)
* Disabling insertion-order preservation (SET preserve_insertion_order=false)
* Increasing the memory limit (SET memory_limit='...GB')

See also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads | fdaId=fda1 | durationMs=118429 | msg=Job failed: refresh-fda
===DuckDB memory===
[]
======
```

I included the `DuckDb` environment info before so we can see what we're working with. \
When the transformation step starts we see the first logs. Initially we only see the tags _COLUMN_DATA_, _CSV_READER_ and
_ALLOCATOR_, nothing strange. _COLUMN_DATA_ rises fastly and considerably, but it starts spilling to disk as expected, reducing
memory usage and augmenting temporary storage (`memory_mb: 348.25, temp_mb: 371.25`). After that _CSV_READER_ and _ALLOCATOR_
remain modest and stable and _COLUMN_DATA_ ends up with a low memory usage, consistently augmenting that temporary storage
use. \
Here we can observe the problem with our low memory environment. Our _out-of-core Processing_ is working as expected but
we still get a constant high memory usage under the tag _EXTENSION_. The only extension we have loaded is
[httpfs](https://duckdb.org/docs/current/core_extensions/httpfs/overview), an extension to read and writte remote files
in object storage (`S3`). \

Because we only have one extension the problem must be directly related to the functionality of that extension, writting
files in `S3`. This would explain why we have a big memory consumption, we are uploading a partitioned parquet, so
effectively we are creating and writting more files. To test this hypothesis I executed the same `FDA` query leaving out
key steps of the full `FDA` creation process:

```javascript
// 1. Basic query, only select
return conn.run(`
    SELECT COUNT(*)
    FROM (${sourceQuery}) AS fda_source
  `);
```

First we execute the query but we dont process the result, we don't writte files and we don't connect with `S3`.

```javascript
// 2. Local parquet
return conn.run(
    `COPY ( SELECT ${cols}
                FROM (${sourceQuery}) AS fda_source)
      TO '/tmp/test.parquet' (FORMAT PARQUET);`,
);
```

After that we add the writting proccess but we create the _parquet_ file locally. With this query we put to work
`DuckDb` creation and transformation logic without involving our extension.

```javascript
// 3. S3 parquet
return conn.run(
    `COPY ( SELECT ${cols}
                FROM (${sourceQuery}) AS fda_source)
      TO 's3://${resultPath}' (FORMAT PARQUET);`,
);
```

After that we change the destionation of the output file. Now we don't writte it locally, we use `httpfs` extension to
writte the parquet file in `S3`.

```javascript
// 4. complete query (parquet + partition)
return conn.run(
    `COPY ( SELECT ${cols}
                FROM (${sourceQuery}) AS fda_source) 
      TO 's3://${resultPath}' (FORMAT PARQUET ${partitionBy} ${compressionString});`,
);
```

This is our normal final query present in the code. In this step we add the partitioning so the extension has the full,
complex scenario present in production. The logs used through this document are from this query. \

After executing this queries we got the following logs:

1. Basic query, only select

```
===DuckDB memory===
[
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 1.041015625, temp_mb: 0 }
]
======
```

2. Local parquet

```
===DuckDB memory===
[
  { tag: 'COLUMN_DATA', memory_mb: 74.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 198.25, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 43.82862949371338, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 129.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 }
]
======
[
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 3.25, temp_mb: 0 }
]
======
```

3. S3 parquet

```
===DuckDB memory===
[
  { tag: 'COLUMN_DATA', memory_mb: 188, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 }
]
======
[
  { tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 61.0390625, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 48.75, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 113.5, temp_mb: 0 },
  { tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 }
]
======
[
  { tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 13.5390625, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 9.75, temp_mb: 0 }
]
======
```

As we see this queries takes far less resources and time than the complete query and don't throw the _OOM_ error. We
only see the _EXTENSION_ tag in the third query, after we introduced connection to `S3` storage system and it uses far
less memory than our full query.

### DuckDB httpfs memory investigation

To see if there's some configuration we can use to manage the `httpfs` memory consumption I investigated the
[DuckDB `httpfs` S3 upload implementation](https://github.com/duckdb/duckdb-httpfs). The investigation showed that S3
uploads use `S3UploadSession`, which allocates multipart-upload buffers through DuckDB's buffer manager using the
`EXTENSION` memory tag.

The uploader uses an adaptive multipart strategy: as more parts are reserved, the target part size can increase. These
buffers are therefore accounted for as `EXTENSION` memory and are **not necessarily spillable to disk**, unlike query
intermediate data such as `COLUMN_DATA`.

Testing confirmed that the problem is specifically associated with **partitioned S3 writes**. Non-partitioned S3 →
Parquet exports completed successfully, while partitioned exports reached approximately 306 MB of `EXTENSION` memory and
failed when DuckDB was limited to ~500 MB, despite temporary storage being used for other query data.

I tried the same operation changing the following `S3` variables to try and reduce the extension memory usage, but
without success: `partitioned_write_max_open_files=1`, `s3_uploader_thread_limit=1` and
`s3_uploader_max_filesize='80GB`.

Finally, we cannot configure how much memory `httpfs` extension uses, so we are gonna increase the total memory limit to
see the full memory usage and have a complete understanding of the needs of the extension:

```
time=2026-09-07T16:36:51.868Z | lvl=INFO | corr=8d63e7ab-fa2b-43fa-bba6-db3903550008 | trans=40034e32-51c6-45eb-afc3-4b91e2dbce79 | op=n/a | ver=1.5.0-next | comp=FDA | srv=postgres | subsrv=/public | setting=memory_limit | value=953.6 MiB | msg=DuckDB setting   <-- increased FDA_DUCKDB_MEMORY_LIMIT=1GB

.
.
.

===DuckDB memory===
[
  { tag: 'COLUMN_DATA', memory_mb: 647, temp_mb: 16.15625 },
  { tag: 'EXTENSION', memory_mb: 153, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 18.8388671875, temp_mb: 0 }
]
======
[
  { tag: 'EXTENSION', memory_mb: 229.5, temp_mb: 0 },
  { tag: 'COLUMN_DATA', memory_mb: 223.5, temp_mb: 52.53125 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 16.22021484375, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 534.5, temp_mb: 349.3125 },
  { tag: 'EXTENSION', memory_mb: 306, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 42.527482986450195, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 }
]
======
[
  { tag: 'COLUMN_DATA', memory_mb: 519.5, temp_mb: 235 },
  { tag: 'EXTENSION', memory_mb: 382.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 21.2998046875, temp_mb: 0 }
]
======
[
  { tag: 'EXTENSION', memory_mb: 76.5, temp_mb: 0 },
  { tag: 'CSV_READER', memory_mb: 30.51953125, temp_mb: 0 },
  { tag: 'ALLOCATOR', memory_mb: 1, temp_mb: 0 }
]
======
```

As we see the total memory usage of the _EXTENSION_ now rises a little bit more (_memory_mb: 382.5_) but after that the
memory is freed and the `FDA` creation finnish succesfully.
