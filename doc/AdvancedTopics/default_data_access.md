# Default Data Access

## Overview

When an FDA is created, the API can also create a built-in DA named `defaultDataAccess`.

This DA is intended to provide an immediate, generic query surface for the whole FDA without requiring the client to
manually define a first DA.

Note that once created this DA is like anything other (i.e. any other created by the user using the proper API
operation). Thus, it can be deleted, etc. using API operations related with DA management.

## Creation rules

Default DA creation is enabled by default and can be controlled in two ways:

-   `FDA_CREATE_DEFAULT_DATA_ACCESS` environment variable defines the instance default behavior. Its default value is
    `true`.
-   `POST /{visibility}/fdas?defaultDataAccess=false` disables it for a specific FDA creation request, overriding the
    instance default for that request only.

Default DA creation only applies to cached FDAs. If an FDA is created with `cached=false`, no parquet bootstrap is
generated and no DAs are created for that FDA.

If enabled, the DA is created automatically after the one-row bootstrap parquet is generated and before the async fetch
job is scheduled.

## Atomic behavior

FDA provisioning is atomic with respect to default DA creation.

If default DA creation fails:

-   bootstrap objects are removed
-   the FDA metadata document is removed
-   the whole `POST /fdas` operation fails

This keeps the system from persisting partially provisioned FDAs.

## Generated query shape

The generated DA is persisted like any other DA. It can later be queried, updated or deleted through the regular DA API.

The query follows this pattern:

```sql
SELECT *, COUNT(*) OVER() as __total
WHERE ($col1 IS NULL OR col1 IN (SELECT CAST(value AS VARCHAR) FROM unnest(string_split($col1, ',')) AS split(value)))
    AND ($col2 IS NULL OR col2 IN (SELECT CAST(value AS VARCHAR) FROM unnest(string_split($col2, ',')) AS split(value)))
LIMIT CAST($pageSize AS BIGINT)
OFFSET CAST($pageStart AS BIGINT)
```

The generated DA includes `__total` via `COUNT(*) OVER()` so clients can read total row count for pagination flows.

Each FDA column gets one optional filter parameter with:

-   `required: false` implicitly
-   `default: null`
-   no explicit `type`

When **real** column types are known (PostgreSQL-backed FDAs, introspected live from the database at creation time), the
generated filter accepts either a single value or a comma-separated list of values. Values are cast to the corresponding
column type before applying `IN`.

When real column types are not known, the generated filter uses a simple equality comparison (`column = $param`)
instead. This avoids binder errors, since the generator cannot determine the target column types required to cast values
produced by `string_split()`. This is the case for uploaded CSV/XLS FDAs (whose schema is only known once the file has
been materialized, after the Default DA has been generated) and also for **every** MongoDB-backed FDA — see the note
below.

#### The comma is a reserved separator

`string_split($param, ',')` splits on every comma, with no escaping or quoting mechanism, so a comma can never be part
of a value in these generated filters. Two consequences worth knowing:

-   **Decimal numbers must use a dot**, never a comma, whatever the locale: `?temperature=33.5`. Sending
    `?temperature=33,5` is not an error — it is read as the two values `33` and `5`, and returns the rows matching
    either of them. This fails **silently**, with a `200` and the wrong rows.
-   **Text values containing a comma cannot be matched at all.** `?name=Doe,%20John` is split into `Doe` and ` John`, so
    the query returns no rows. There is no way to escape it in the generated filter; query such a column through a
    custom DA using plain equality (`WHERE "name" = $name`) instead.

A value that cannot be cast to the column type (for example `?age=abc` on an `INTEGER` column) fails loudly instead,
with a DuckDB conversion error.

Filters generated **without** real column types (MongoDB-backed and uploaded FDAs, see below) use plain equality, so
none of this applies to them: their values may contain commas, and they accept a single value only.

### MongoDB-backed FDAs

MongoDB does not provide a fixed schema to introspect, so cached FDAs in `strict` mode take their columns from the
query's declared output (`projection` or final `$project`/`$group` stage). Types remain unknown until the first fetch:

```json
[
    { "name": "device", "type": null },
    { "name": "reading", "type": null }
]
```

After the first fetch, the schema is re-derived from the materialized Parquet.

Because the Default DA is generated before the first fetch, MongoDB FDAs use simple equality filters (`column = $param`)
instead of typed `IN (...)` filters.

For custom DAs, parameters can still be used according to the actual data type stored in Parquet. If a MongoDB field can
contain different types across documents, an explicit `CAST` may be required for comparisons involving that field.

## Time range support

If the FDA defines `timeColumn`, the generated DA also adds:

-   `start`
-   `finish`

These are optional range filters applied to the FDA time column.

Current generated predicate shape:

```sql
($start IS NULL OR CAST(timeColumn AS TIMESTAMP) >= CAST($start AS TIMESTAMP))
($finish IS NULL OR CAST(timeColumn AS TIMESTAMP) <= CAST($finish AS TIMESTAMP))
```

Important note about temporal columns:

-   If an FDA includes a temporal column that is not declared as timeColumn, that column is treated as a regular
    optional filter. If its type is available, the generated default DA casts the split values to that temporal type
    before applying `IN`; otherwise it keeps the old equality comparison shape.
-   For reliable temporal filtering, declare the FDA timeColumn and use start and finish parameters.
-   If a column is declared as `timeColumn`, the equality filter shape is:
    `($${paramName} IS NULL OR DATE_TRUNC('millisecond', CAST(${quotedColumnName} AS TIMESTAMP)) = DATE_TRUNC('millisecond', CAST($${paramName} AS TIMESTAMP)))`.

## Pagination support

Default DA also includes two optional pagination parameters:

-   `pageSize` (default `9223372036854775807`)
-   `pageStart` (default `0`)

They are always present.

About `9223372036854775807` in `LIMIT`:

-   This value is the maximum signed 64-bit integer (`BIGINT`), i.e. `2^63 - 1`.
-   It is used as an "effectively unbounded" default value for `pageSize`.
