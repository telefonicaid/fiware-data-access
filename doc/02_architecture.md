# Architecture

This document describes the system architecture of **FIWARE Data Access**, including main concepts and database model.

---

## Concepts

This section explains what an **FDA** and a **DA / CDA** are in the context of the FIWARE Data Access API.

### FDA

An **FDA** represents a **materialized dataset** in the system. It:

-   Defines how data is extracted from a **source database** (PostgreSQL)
-   Executes a **base SQL query**
-   Stores the result as a **Parquet file**
-   Saves the file in a **bucket-based object storage system**
-   Acts as the **base dataset** for one or more DAs

In simple terms:

> **An FDA is a reusable, precomputed snapshot of data.**

Key characteristics:

-   Created from a base SQL query
-   Physically stored as a Parquet file
-   Stored inside a bucket named after the `Fiware-Service`
-   Can be regenerated to refresh the data
-   Parent resource of one or more DAs
-   Has status, progress and lastExecution fields.

#### Example FDA

```http
POST /fdas
Fiware-Service: acme
Content-Type: application/json
```

```json
{
    "id": "animals_fda",
    "description": "All animal activity records",
    "query": "SELECT * FROM animal_activity"
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
POST /fdas/animals_fda/das
Fiware-Service: acme
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
-   **MongoDB**: stores metadata about FDAs and DAs
-   **DuckDB**: executes queries over Parquet files stored in MinIO

Data flow:

1. FDA is created from PostgreSQL → CSV → Parquet → uploaded to MinIO
2. Metadata about the FDA and its DAs is stored in MongoDB
3. DA queries are executed on Parquet datasets using DuckDB
4. Results are returned as JSON via the API

---

## Database Model

`Fiware-Data-Access` uses a single MongoDB collection named **fdas** to manage all FDA and DA metadata.

### FDAs collection

Each document corresponds to one FDA:

-   **\_id**: MongoDB unique identifier
-   **fdaId**: FDA identifier
-   **service**: FIWARE service (`fiware-service`) name
-   **servicePath**: FIWARE service path (`fiware-servicePath`) for access control
-   **description**: FDA description
-   **query**: SQL query used to generate the Parquet file
-   **das**: keymap of DAs associated with the FDA

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
    "servicePath": "/public",
    "das": {
        "da1": {
            "description": "First DA querying timeInstant and population.",
            "query": "SELECT * WHERE population = $population AND timeinstant = $timeinstant;"
        },
        "da2": {
            "description": "Second DA querying timeInstant and gender.",
            "query": "SELECT * WHERE gender = $gender AND timeinstant = $timeinstant;"
        }
    },
    "service": "fiwareService"
}
```

---

## Key Points

-   The **bucket name** in MinIO must match the `fiware-service` of the FDA
-   FDA creation **does not create buckets automatically**; they must exist beforehand
-   DAs query the FDA Parquet files directly via DuckDB
-   Metadata in MongoDB ensures unique combination of `fdaId` and `service`

---

## 🧭 Navigation

-   [⬅️ Previous: Installation](/doc/01_installation.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: API](/doc/03_api.md)
