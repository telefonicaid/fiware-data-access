# SQL output for only-fresh FDAs

## Background

Dumping a TimescaleDB hypertable, or just a subset of it, is not straightforward with `pg_dump`. Only-fresh FDAs
(`cached=false`) already run their DAs straight against PostgreSQL, so any DA result can be downloaded as a plain-text
SQL script, similar to the one `pg_dump` writes, that recreates it as a table in another PostgreSQL or TimescaleDB
database.

SQL output is only available for **only-fresh PostgreSQL FDAs**. Cached FDAs (whose data lives in Parquet files) and
MongoDB FDAs (which have no column types) reject it with `400 UnsupportedOutputType`, before any download is counted.

## Requesting a SQL dump

SQL output is one more format of `GET /{visibility}/fdas/{fdaId}/das/{daId}/data`, selected like the others:

-   query-style: `outputType=sql`;
-   header-style: `Accept: application/sql`.

```bash
curl -o measures.sql "http://localhost:8080/public/fdas/fda_measures/das/defaultDataAccess/data?service=trantor&servicePath=%2FservicePath&outputType=sql&sqlSchema=backups&sqlHypertable=timeinstant"
```

```bash
curl -o measures.sql "http://localhost:8080/public/fdas/fda_measures/das/da_last_day/data?sqlFormat=copy" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath" \
    -H "Accept: application/sql"
```

The response is streamed with `Content-Type: application/sql; charset=utf-8` and
`Content-Disposition: attachment; filename="results.sql"`, and counts as a download like any other format.

## Parameters

These query parameters shape the script. They are optional, they are only accepted with SQL output (`400 BadRequest`
with any other format), and they are never passed to the DA as params.

| Parameter       | What it does                                                                           | Default                                                |
| --------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `sqlTable`      | Name of the table the script creates                                                   | `<fdaId>_<daId>`, or `<fdaId>` for `defaultDataAccess` |
| `sqlSchema`     | Schema of the table; the script creates it if it does not exist                        | None (the restore session `search_path`)               |
| `sqlHypertable` | Column used as time dimension to turn the table into a hypertable                      | No hypertable                                          |
| `sqlFormat`     | How rows are written: `insert` (`INSERT` statements) or `copy` (`COPY ... FROM stdin`) | `insert`                                               |

`sqlTable` and `sqlSchema` must have 1 to 63 alphanumeric characters, hyphens or underscores. `sqlHypertable` must be
one of the columns returned by the query, with the same spelling.

## What the script contains

For a DA returning `id`, `label` and `measured_at`, requested with `sqlSchema=backups&sqlHypertable=measured_at`:

```sql
-- FIWARE Data Access SQL dump

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

BEGIN;

CREATE SCHEMA IF NOT EXISTS "backups";

CREATE TABLE "backups"."fda_measures" (
    "id" integer,
    "label" character varying(50),
    "measured_at" timestamp with time zone
);

SELECT create_hypertable('"backups"."fda_measures"', by_range('measured_at'));

INSERT INTO "backups"."fda_measures" ("id", "label", "measured_at") VALUES
('1', 'plain', '2026-11-19 10:00:00+00'),
('2', 'it''s', NULL);

COMMIT;
```

-   **Column types** are the exact ones PostgreSQL reports for the query: `numeric(10,2)`, `varchar(50)`,
    `timestamp(3)`, arrays, `jsonb`, types from extensions such as PostGIS... User-defined types and extensions must
    exist in the target database.
-   **Values** are written as PostgreSQL itself prints them as text, so dates, `numeric` scale, JSON, arrays, `bytea`
    and 64-bit integers are restored exactly.
-   **Rows** are written as one multi-row `INSERT` per chunk read from the source (250 rows), or as a single
    `COPY ... FROM stdin` block closed by `\.`.
-   Column names returned by the query must be unique (`400 BadRequest` otherwise).

## INSERT or COPY

|                     | `sqlFormat=insert` (default)                      | `sqlFormat=copy`                                   |
| ------------------- | ------------------------------------------------- | -------------------------------------------------- |
| Restorable with     | Any client: `psql`, DBeaver, pgAdmin, a driver... | `psql` only (`\.` and inline data are psql syntax) |
| Size and load speed | Bigger and slower                                 | Smaller and much faster                            |
| Same as `pg_dump`   | `pg_dump --inserts`                               | `pg_dump` default                                  |

Use `copy` for big dumps restored with `psql`, and `insert` when the dump has to be run from a GUI client.

## TimescaleDB hypertables

With `sqlHypertable=<column>` the script turns the table into a hypertable on that column right after creating it and
before loading the rows:

```sql
SELECT create_hypertable('"backups"."fda_measures"', by_range('measured_at'));
```

-   It needs TimescaleDB **2.13 or later** in the target database, where `by_range` was introduced. It has been tested
    with TimescaleDB 2.28.
-   The `timescaledb` extension must already be created in the target database (`CREATE EXTENSION timescaledb`). The
    script does not create it, because that needs privileges the restoring user may not have.
-   Restored on PostgreSQL without TimescaleDB, the script fails at `create_hypertable` and nothing is left behind (see
    below). Without `sqlHypertable` the same dump restores in plain PostgreSQL and in TimescaleDB as a normal table.

## Transactions, failures and restoring twice

The whole script runs in one `BEGIN` / `COMMIT` transaction:

-   If any statement fails while restoring (missing extension, missing type, existing table...), the transaction is
    rolled back and the target database is left as it was.
-   If the query fails in the middle of the stream, the response is cut off and the file has no `COMMIT`. Restoring it
    rolls everything back, so a truncated dump never restores half the rows.
-   The script never drops or empties anything. It uses `CREATE TABLE` without `IF NOT EXISTS`, so restoring a dump into
    a table that already exists fails and does not load the rows a second time. To restore it again, drop the table
    first or choose another `sqlTable` / `sqlSchema` when downloading it.

## Restoring

```bash
psql -h target-host -U postgres -d target_db -f measures.sql
```

Or straight from the API:

```bash
curl -s "http://localhost:8080/public/fdas/fda_measures/das/defaultDataAccess/data?sqlFormat=copy" \
    -H "Fiware-Service: trantor" -H "Fiware-ServicePath: /servicePath" -H "Accept: application/sql" \
  | psql -h target-host -U postgres -d target_db
```

`insert` dumps can also be opened and run from DBeaver, pgAdmin or any other SQL client.

## Errors

| Status | Error                   | When                                                                                       |
| ------ | ----------------------- | ------------------------------------------------------------------------------------------ |
| 400    | `UnsupportedOutputType` | SQL output requested on a cached FDA or on an only-fresh MongoDB FDA                       |
| 400    | `BadRequest`            | SQL params sent with another format, invalid `sqlTable` / `sqlSchema` / `sqlFormat` values |
| 400    | `BadRequest`            | `sqlHypertable` column not returned by the query, or duplicated column names               |

Errors detected after the download is claimed (unknown `sqlHypertable` column, duplicated column names) give the
download back, as any other query that fails before sending data.
