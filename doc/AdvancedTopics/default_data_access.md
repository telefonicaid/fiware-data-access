# Default Data Access

## Overview

When an FDA is created, the API can also create a built-in DA named `defaultDataAccess`.

This DA is intended to provide an immediate, generic query surface for the whole FDA without requiring the client to
manually define a first DA.

## Creation rules

Default DA creation is enabled by default and can be controlled in two ways:

-   `FDA_CREATE_DEFAULT_DATA_ACCESS` environment variable defines the instance default behavior.
-   `POST /{visibility}/fdas?defaultDataAccess=false` disables it for a specific FDA creation request.

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

## Pagination support

Default DA also includes two optional pagination parameters:

-   `limit`
-   `offset`

They are always present.

## Current limitation in fresh mode

The optional-filter pattern works correctly in cached mode over DuckDB/Parquet.

However, it is currently not guaranteed to work in `fresh=true` mode over PostgreSQL for typeless optional parameters,
particularly with predicates shaped as:

```sql
($p IS NULL OR col = $p)
```

This limitation is currently accepted and pending design discussion.
