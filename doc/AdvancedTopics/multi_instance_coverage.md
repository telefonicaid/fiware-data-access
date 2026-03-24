# Multi-Instance Coverage and Operational Considerations

This document describes the current multi-instance behavior of FDA and the key operational points to consider in
production deployments.

---

## Current Multi-Instance Situation

FDA is designed to run safely with multiple replicas by separating API responsibilities from background processing and
by using shared persistence for state and jobs.

Main pillars:

1. MongoDB as source of truth for FDA metadata and job persistence.
2. Agenda for distributed background execution and lock handling.
3. Role-based runtime configuration to split API and fetcher workloads.
4. Per-instance concurrency guard for fresh PostgreSQL queries.
5. Connection pooling on both DuckDB (object storage side) and PostgreSQL (source side).

---

## Key Coverage Areas

## 1. Distributed coordination for fetch and refresh

Heavy operations are not executed in request handlers. API instances schedule jobs and fetcher instances execute them.

-   Job scheduling and status transitions are implemented in [src/lib/fda.js](../../src/lib/fda.js).
-   Job backend and persistence are configured in [src/lib/jobs.js](../../src/lib/jobs.js).
-   Worker-side job execution is implemented in [src/fetcher.js](../../src/fetcher.js).

Important consequence: multiple fetcher replicas can coexist while keeping execution coordination through Agenda locks.

## 2. Role-based scaling model

Instances can run as API-only, fetcher-only, or mixed mode:

-   `FDA_ROLE_APISERVER`
-   `FDA_ROLE_FETCHER`
-   `FDA_ROLE_SYNCQUERIES`

Configuration is defined in [src/lib/fdaConfig.js](../../src/lib/fdaConfig.js) and documented in
[doc/04_config_operational_guide.md](../04_config_operational_guide.md).

This allows independent scaling of request serving and data refresh workloads.

## 3. Query behavior during lifecycle states

FDA queryability is explicitly tied to first successful fetch completion:

-   First provisioning creates a one-row parquet synchronously.
-   Query execution is blocked until `lastFetch` exists (`409 FDAUnavailable`).
-   After first completion, reads can continue using the latest available parquet snapshot while refresh jobs run.

This behavior is implemented in [src/lib/fda.js](../../src/lib/fda.js).

## 4. Fresh query pressure control

Fresh mode (`GET /query?fresh=true`) executes DA queries directly in PostgreSQL. To avoid API overload, FDA applies a
concurrency cap per API instance:

-   `FDA_MAX_CONCURRENT_FRESH_QUERIES`
-   Exceeded limit returns `429 TooManyFreshQueries`

Guard logic is implemented in [src/lib/utils/utils.js](../../src/lib/utils/utils.js).

Note: this limit is per instance, not global across replicas.

## 5. PostgreSQL pooled connections

PostgreSQL operations now use pooled connections with reuse and idle cleanup:

-   Pooling logic is implemented in [src/lib/utils/pg.js](../../src/lib/utils/pg.js).
-   Pools are maintained per target database and reused across calls.
-   Clients are acquired with `pool.connect()` and released with `release()`.
-   All pools are closed during graceful shutdown.

Tuning variables:

-   `FDA_PG_POOL_MAX`
-   `FDA_PG_POOL_IDLE_TIMEOUT_MS`
-   `FDA_PG_POOL_CONN_TIMEOUT_MS`

---

## Operational Points to Keep in Mind

1. Per-instance limits are not global limits.
2. API/fetcher ratio should be adjusted according to job backlog and query traffic profile.
3. PostgreSQL pool size should be tuned with replica count in mind to avoid exhausting DB connections.
4. Fresh queries and refresh jobs compete for PostgreSQL capacity; monitor both together.
5. Graceful shutdown is important in orchestrated environments to drain jobs and release pooled resources cleanly.

---

## Recommended Monitoring Signals

1. Agenda backlog and job latency.
2. Number of fetch jobs in `fetching` and failed states.
3. Rate of `429 TooManyFreshQueries` responses.
4. PostgreSQL connection usage at server level.
5. Average and p95 duration for refresh and fresh-query executions.

These signals give early visibility into scaling pressure before user-facing failures appear.
