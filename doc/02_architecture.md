
# Architecture

This document describes the system architecture of **FIWARE Data Access**, including main concepts and database model.

---

## Concepts

This section explains what an **FDA** and a **DA / CDA** are in the context of the FIWARE Data Access API.

<img width="1536" height="1024" alt="FDA_diagram" src="https://github.com/user-attachments/assets/6f6a99bd-4925-484d-84d0-a0d91edbf397" />

### Datasource

A **Datasource** represents the connection definition FDA uses to access source systems.

In the current implementation:

-   Datasources are scoped by `Fiware-Service` and identified by `datasourceId`
-   FDAs store `datasourceId` and resolve credentials at execution time
-   Supported datasource type is currently `postgres`

In simple terms:

> **A datasource is the reusable connection profile used by FDAs to fetch fresh/source data.**

### FDA

An **FDA** represents a **materialized dataset** in the system. It:

-   Defines how data is extracted from a **source database** (PostgreSQL)
-   Executes a **base SQL query**
-   Stores the result as a **Parquet file**
-   Saves the file in a **bucket-based object storage system**
-   Acts as the **base dataset** for one or more DAs
-   Supports an optional `refreshPolicy` that defines how the FDA is automatically refreshed (none, interval or window)

In simple terms:

> **An FDA is a reusable, precomputed snapshot of data.**

Key characteristics:

-   Created from a base SQL query
-   Physically stored as a Parquet file
-   Stored inside a bucket named after the `Fiware-Service`
-   Associated with a `visibility` level (`public` or `private`) that controls access authorization
-   Can be regenerated (manually or configured) to refresh the data
-   Parent resource of one or more DAs
-   Has status, progress and lastFetch fields, and an optional refreshPolicy.

#### Example FDA

```http
POST /{visibility}/fdas
Fiware-Service: acme
Fiware-ServicePath: /servicePath
Content-Type: application/json
```

```json
{
    "id": "animals_fda",
    "description": "All animal activity records",
    "query": "SELECT * FROM animal_activity",
    "refreshPolicy": {
        "type": "interval",
        "value": "0 * * * *"
    }
}
```

---

### DA / CDA

A **DA (Data Access)**, also known as **CDA** in legacy Pentaho systems, is a **logical, parameterized query** executed
on top of an FDA.

It:

-   Does **not** store data
-   Defines how to query an existing FDA
-   Can be executed multiple times with different parameters
-   Returns results in JSON format
-   Does **not** include **FROM** clause. Access the FDA directly, you don't need to know the internal storage logic

In simple terms:

> **A DA (or CDA) is a saved analytical query over an FDA.**

Key characteristics:

-   Always associated with exactly one FDA
-   Contains a query definition
-   Supports URL parameters
-   Produces JSON query results
-   Used by dashboards, services, or external clients

#### Example DA

```http
POST /{visibility}/fdas/animals_fda/das
Fiware-Service: acme
Fiware-ServicePath: /servicePath
Content-Type: application/json
```

```json
{
    "id": "activity_by_species",
    "description": "Activity filtered by animal species",
    "query": "SELECT * WHERE animalspecies = ${species}"
}
```

---

## Architecture Overview

The **FIWARE Data Access** system consists of:

-   **Node.js FDA server**: handles API requests, processes FDA and DA objects, orchestrates data flow
-   **PostgreSQL**: source database for FDAs
-   **MinIO**: object storage for Parquet files
-   **MongoDB**: stores metadata about FDAs, DAs, and datasources
-   **DuckDB**: executes queries over Parquet files stored in MinIO

Data flow:

1. FDA is created from PostgreSQL → CSV → Parquet → uploaded to MinIO
2. Metadata about the FDA and its DAs is stored in MongoDB
3. FDA source/fresh execution resolves datasource credentials from MongoDB by `service + datasourceId`
4. DA queries are executed on Parquet datasets using DuckDB
5. Results are returned as JSON via the API

Fresh mode:

-   DAs are always executed on the cached parquet snapshot.
-   For use cases that require real-time data, `GET /{visibility}/fdas/{fdaId}/data` executes the FDA directly on
    PostgreSQL.
-   FDAs can be created with `cached=false` to disable parquet generation and operate as only-fresh resources.
-   Fresh mode is controlled per instance with `FDA_ROLE_SYNCQUERIES`.

---

## Database Model

`Fiware-Data-Access` uses MongoDB collections for FDA/DA metadata and datasource provisioning.

### FDAs collection

Each document corresponds to one FDA:

-   **\_id**: MongoDB unique identifier
-   **fdaId**: FDA identifier
-   **service**: FIWARE service (`fiware-service`) name
-   **servicePath**: FIWARE service path (`fiware-servicePath`) for access control
-   **visibility**: FDA access visibility level (`public` or `private`)
-   **description**: FDA description
-   **query**: SQL query used to generate the Parquet file
-   **das**: keymap of DAs associated with the FDA
-   **refreshPolicy**: object defining automatic refresh behaviour (`none`, `interval`, or `window`)
-   **status**: current execution status (`fetching`, `transforming`, `uploading`, `completed`, `failed`)
-   **progress**: execution progress percentage (0–100)
-   **lastFetch**: timestamp of the last fetch (ISO date)
-   **datasourceId**: datasource identifier used to resolve source credentials (default `default` when omitted)

Each DA contains:

-   **description**: description of the DA
-   **query**: parameterized SQL query executed on the FDA

#### Example MongoDB document

```json
{
    "_id": "695f9a3cc0d41d928f5e6a39",
    "fdaId": "fda1",
    "description": "Description for the first FDA",
    "query": "SELECT population, timeinstant FROM exampleSchema.exampleTable",
    "servicePath": "/servicePath",
    "service": "fiwareService",
    "progress": 10,
    "lastFetch": null,
    "visibility": "public",
    "refreshPolicy": {
        "type": "interval",
        "value": "1 hour"
    },
    "das": {
        "da1": {
            "description": "First DA querying timeInstant and population.",
            "query": "SELECT * WHERE population = $population AND timeinstant = $timeinstant;"
        },
        "da2": {
            "description": "Second DA querying timeInstant and gender.",
            "query": "SELECT * WHERE gender = $gender AND timeinstant = $timeinstant;"
        }
    }
}
```

### Datasources collection

Each document corresponds to one datasource definition for one service:

-   **service**: FIWARE service (`fiware-service`) name
-   **datasourceId**: datasource identifier (unique within the service)
-   **type**: datasource type (currently `postgres`)
-   **config**: connection settings (for postgres: `user`, `password`, `host`, `port`, `database`)

#### Example MongoDB datasource document

```json
{
    "_id": "6960aa3cc0d41d928f5e6b99",
    "service": "fiwareService",
    "datasourceId": "default",
    "type": "postgres",
    "config": {
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "fiwareService"
    }
}
```

### Agenda Jobs Collection

When automatic refresh or manual regeneration is enabled, the system uses Agenda with MongoDB as a backend.

Agenda persists jobs in a separate MongoDB collection:

-   **Collection name**: `agendaJobs`
-   **Purpose**: Stores background jobs for FDA refresh and regeneration
-   **Managed by**: Agenda (not manually modified by application code)

Each document represents one scheduled or running job and includes:

-   Job metadata (`name`, `data`, `nextRunAt`)
-   Locking information (`lockedAt`, `lockLifetime`)
-   Retry tracking (`failCount`, `failReason`)
-   Execution timestamps (`lastRunAt`, `lastFinishedAt`)

> This collection is internal to Agenda and should not be modified directly.

> More details about asynchronous processing and job persistence can be found in
> [Advanced Topics – Async Processing and Jobs](/doc/AdvancedTopics/async_processing_and_jobs.md#6-job-persistence-in-mongodb).

---

## Key Points

-   The **bucket name** in MinIO must match the `fiware-service` of the FDA
-   FDA creation **does not create buckets automatically**; they must exist beforehand
-   DAs query the FDA Parquet files directly via DuckDB
-   Metadata in MongoDB ensures unique combination of `fdaId` and `service`
-   Datasources are managed per `Fiware-Service`; deleting a datasource referenced by at least one FDA is blocked with
    `DatasourceInUse`

---

## 🧭 Navigation

-   [⬅️ Previous: Installation](/doc/01_installation.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: API](/doc/03_api.md)
