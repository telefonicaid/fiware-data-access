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
SELECT *
WHERE ($col1 IS NULL OR col1 = $col1)
  AND ($col2 IS NULL OR col2 = $col2)
LIMIT CAST(COALESCE($limit, 9223372036854775807) AS BIGINT)
OFFSET CAST(COALESCE($offset, 0) AS BIGINT)
```

About `9223372036854775807` in `LIMIT`:

-   This value is the maximum signed 64-bit integer (`BIGINT`), i.e. `2^63 - 1`.
-   It is used as an "effectively unbounded" limit when `limit` is not provided.
-   The generated query uses `COALESCE($limit, ...)` because SQL does not support expressing `LIMIT` with an
    `($limit IS NULL OR ...)` pattern.

Each FDA column gets one optional equality filter parameter with:

-   `required: false` implicitly
-   `default: null`
-   no explicit `type`

Parameter names are sanitized to alphanumeric and underscore format. If a generated name collides with reserved
parameters, a numeric suffix is added.

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
    optional equality filter.
-   In that case, exact equality comparisons may be unreliable because of timestamp precision and representation
    differences.
-   For reliable temporal filtering, declare the FDA timeColumn and use start and finish parameters.
-   If a column is declared as `timeColumn`, the equality filter shape is:
    `($${paramName} IS NULL OR DATE_TRUNC('millisecond', CAST(${quotedColumnName} AS TIMESTAMP)) = DATE_TRUNC('millisecond', CAST($${paramName} AS TIMESTAMP)))`.

## Pagination support

Default DA also includes two optional pagination parameters:

-   `limit`
-   `offset`

They are always present.
