# Advanced Topics

## Introduction

The `FDA` app interacts and communicates with various systems. To keep that communication tidy we have multiple
documentation files describing the different structures and functionalities involved. This file documents the advanced
topics and FDA internals that don't fit in the other documents.

## Object Bucket-Based Storage System

`FDA` uses an object bucket-based storage system to efficiently store the data to query. In this section we have the
topics related to this system.

-   [Bucket name convention](#bucket-name-convention)
-   [File name convention](#file-name-convention)
-   [Data origin](#data-origin)

### Bucket name convention

When creating a _FDA_ we fetch the data from `PostgreSQL` and store inside a _bucket_ in our storage system. The name of
this _bucket_ is fixed and is the **same** as the `fiware-service` of the _FDA_. \
The `FDA` app **does not** create the bucket when uploading a _FDA_ to the object bucket-based storage system, it instead
rises an error. This is because we want to manage the bucket permissions so we prefer to create the bucket previously by
hand.

### File name convention

The `PostgreSQL` data is always uploaded to the `Object Bucket-Based Storage System` in a _parquet_ file using the
_fdaId_ as the name. This file is updated and removed in unity with the _fda_ information in `MongoDb`.

### Data origin

When creating a _FDA_ we cannot specify the database from where we want to fetch the data. In _FDA_ we are always gonna
use the `fiware-service` value as the _database_ name.

---

## FDA Execution Lifecycle

Each `FDA` has **operational fields** (`status`, `progress`, `lastFetch`) that are **read-only** for clients and reflect
its asynchronous processing state.

### Status & Progress

| Status         | Progress | Description                                      |
| -------------- | -------- | ------------------------------------------------ |
| `fetching`     | 0–20     | Data is being retrieved from the source.         |
| `transforming` | 20–60    | Data is being transformed (e.g., CSV → Parquet). |
| `uploading`    | 60–80    | Data is being uploaded to object storage.        |
| `completed`    | 100      | Processing finished successfully.                |
| `failed`       | 0        | Processing failed at any step.                   |

### Last Execution

`lastFetch` records the timestamp of the last completed attempt in ISO format.

### Flow

-   On `POST /{visibility}/fdas` or `PUT /{visibility}/fdas/:fdaId`, FDA starts **fetching** (progress 0). If a
    `refreshPolicy` is defined, subsequent refreshes are scheduled via the job system (agenda).
-   `transforming` → `uploading` as processing steps complete.
-   On success → `completed` (progress 100).
-   On error → `failed` (progress 0).

---

## Asynchronous Processing & Job System

`FDA` executes heavy data operations using a background job system based on **agenda**. Jobs are persisted in
**MongoDB** and executed outside the HTTP lifecycle.

This architecture:

-   Decouples API from processing logic
-   Improves scalability
-   Enables state persistence and recovery
-   Provides execution traceability and respects FDA `refreshPolicy` to schedule automatic refreshes.

👉 Full documentation available at:
[`Async Processing & Job Architecture`](/doc/AdvancedTopics/async_processing_and_jobs.md)

### Sliding windows and partitioning

Using **agenda** and it's job system `FDA` supports an special refresh modality. In the sliding window refresh modality
the user can schedule periodic jobs to retrieve data inside a specific time interval (e.g. the data of the last month).
This functionality pairs really well with the _partitioning_ of parquet files and they are intended to be used together
for performance optimization, but the freedom in the configuration granted to the user can cause some problems.

👉 Full documentation available at:
[`Sliding windows and partitioned files`](/doc/AdvancedTopics/sliding_windows_and_partitioning.md)

### Multi-instance coverage

This topic summarizes the current multi-instance architecture and operational considerations: distributed job
coordination, role-based scaling, query lifecycle behavior, fresh-query limits, and connection pooling.

👉 Full documentation available at: [`Multi-instance coverage`](/doc/AdvancedTopics/multi_instance_coverage.md)

---

## Pentaho CDA Compatibility Layer

FDA includes a compatibility layer to support legacy Pentaho CDA clients.

### Endpoint

```
POST /plugin/cda/api/doQuery
```

### Purpose

This layer:

-   Translates CDA-style requests into FDA execution calls
-   Resolves `service` from `path` if not explicitly provided
-   Resolves `fdaId` from `cda` field if provided, otherwise defaults to `dataAccessId`
-   Uses `dataAccessId` as DA identifier
-   Extracts parameters prefixed with `param`
-   Forwards pagination parameters (`pageSize`, `pageStart`) without transformation
-   Adapts FDA results into CDA-compatible format:

    -   `{ metadata, resultset, queryInfo }`

### Architectural Principle

The compatibility layer is intentionally thin.

It:

-   Does **not** enforce query semantics
-   Does **not** modify parameter naming
-   Does **not** inject sorting or pagination logic
-   Does **not** implement operator logic

All query behavior (filters, pagination, sorting) must be explicitly defined at the DA level.

This guarantees:

-   Clean separation between transport and execution
-   Predictable behavior
-   No hidden semantics
-   Backward compatibility without polluting FDA core logic

> In CDA mode, if no explicit `cda` field is provided, the FDA identifier defaults to the same value as `dataAccessId`.
> This allows a 1:1 mapping between legacy CDA definitions and FDA datasets.

### Pagination Behavior

If the DA includes a `__total` field in its result:

```
queryInfo.totalRows = __total
```

Otherwise:

```
queryInfo.totalRows = resultset.length
```

### Limitations

Currently unsupported features:

-   `param_not_` operator
-   Dynamic comparison operators (`>`, `<`, `!=`)
-   Dynamic sorting
-   NDJSON streaming in CDA mode

These may be implemented in future versions if required.

---

## 🧭 Navigation

-   [⬅️ Previous: Config And Operational Guide](/doc/04_config_operational_guide.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Testing](/doc/06_testing.md)
