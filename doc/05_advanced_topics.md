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

`lastFetch` records the timestamp of the last attempt in ISO format.

### Flow

-   On `POST /fdas` or `PUT /fdas/:fdaId`, FDA starts **fetching** (progress 0).
-   `transforming` → `uploading` as processing steps complete.
-   On success → `completed` (progress 100).
-   On error → `failed` (progress 0).

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
