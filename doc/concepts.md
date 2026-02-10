# FDA and CDA in FIWARE Data Access

This document explains what an **FDA** and a **CDA (DA)** are in the context of the **FIWARE Data Access API**, including examples and a clear mental model.  
It is written to be directly usable as project documentation.

---

## Context: FIWARE Data Access

The **FIWARE Data Access component** is part of the **FIWARE** ecosystem and is designed to expose analytical data through reusable and parameterized APIs.

To achieve this, FIWARE defines two main concepts:

- **FDA (FIWARE Data Access)**
- **DA / CDA (Data Access / Community Data Access – legacy Pentaho terminology)**

These concepts are related but serve different roles in the data lifecycle.

---

## What is an FDA?

### Definition

An **FDA** represents a **materialized dataset** in the system.

An FDA:
- Defines how data is extracted from a **source database** (PostgreSQL)
- Executes a **base SQL query**
- Stores the result as a **Parquet file**
- Saves the file in a **bucket-based object storage system**
- Acts as the **base dataset** for one or more DAs

In simple terms:

> **An FDA is a reusable, precomputed snapshot of data.**

---

### Key characteristics of an FDA

- Created from a base SQL query
- Physically stored as a Parquet file
- Stored inside a bucket named after the `Fiware-Service`
- Can be regenerated to refresh the data
- Parent resource of one or more DAs

---

### FDA example

#### Create an FDA

```http
POST /fdas
Fiware-Service: acme
Content-Type: application/json
```

```json
{
  "id": "animals_fda",
  "description": "All animal activity records",
  "database": "farm_db",
  "query": "SELECT * FROM animal_activity",
  "path": "/animals/animals_fda.parquet"
}
```
## What is a DA / CDA?

### Definition

A **DA (Data Access)** — also known as **CDA (Community Data Access)** in legacy Pentaho systems — is a **logical, parameterized query** executed **on top of an FDA**.

A DA:
- Does **not** store data
- Defines how to query an existing FDA
- Can be executed multiple times with different parameters
- Returns results in JSON format

In simple terms:

> **A DA (or CDA) is a saved analytical query over an FDA.**

---

### Key characteristics of a DA / CDA

- Always associated with exactly one FDA
- Contains a query definition
- Supports URL parameters
- Produces JSON query results
- Used by dashboards, services, or external clients

---

### DA / CDA example

#### Create a DA on an FDA

```http
POST /fdas/animals_fda/das
Fiware-Service: acme
Content-Type: application/json
```

```json
{
  "id": "activity_by_species",
  "description": "Activity filtered by animal species",
  "query": "SELECT * FROM fda WHERE animalspecies = ${species}"
}
```

This DA:
- Defines how to filter data from the FDA
- Accepts a species parameter
- Can be reused for multiple queries


### Execute the DA (query)

**FIXME [#62](https://github.com/telefonicaid/fiware-data-access/issues/62)**: this request is going to change when that issue get addressed

```http
GET /query?fdaId=animals_fda&daId=activity_by_species&species=Ovino
Fiware-Service: acme
```

Response example:

```json
{"timeinstant": "2020-08-17T18:25:28.332Z","activity": 12,"animalname": "TUNA","animalspecies": "Ovino"}
{"timeinstant": "2020-08-17T18:26:28.332Z","activity": 18,"animalname": "TUNA","animalspecies": "Ovino"}
{"timeinstant": "2020-08-17T18:27:28.332Z","activity": 47,"animalname": "TUNA","animalspecies": "Ovino"}
{"timeinstant": "2020-08-17T18:28:28.332Z","activity": 21,"animalname": "TUNA","animalspecies": "Ovino"}
```



