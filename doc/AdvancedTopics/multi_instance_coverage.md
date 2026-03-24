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

Agenda guarantees:

-   Distributed locking via MongoDB (`lockedAt` + `lockLifetime`)
-   Single execution per job
-   Safe horizontal scaling without duplicate fetch execution

---

## 2. Role-based scaling model

Instances can run as API-only, fetcher-only, or mixed mode:

-   `FDA_ROLE_APISERVER`
-   `FDA_ROLE_FETCHER`
-   `FDA_ROLE_SYNCQUERIES`

Configuration is defined in [src/lib/fdaConfig.js](../../src/lib/fdaConfig.js) and documented in
[doc/04_config_operational_guide.md](../04_config_operational_guide.md).

This allows independent scaling of:

-   Request serving (API)
-   Background processing (fetchers)
-   Real-time query execution (fresh queries) is, at least at the moment, linked to API instance

---

## 3. Query behavior during lifecycle states

FDA queryability is explicitly tied to first successful fetch completion:

-   First provisioning creates a one-row parquet synchronously.
-   Query execution is blocked until `lastFetch` exists (`409 FDAUnavailable`).
-   After first completion, reads can continue using the latest available parquet snapshot while refresh jobs run.

This behavior is implemented in [src/lib/fda.js](../../src/lib/fda.js).

---

## 4. Fresh query pressure control

Fresh mode (`GET /query?fresh=true`) executes DA queries directly in PostgreSQL.

To avoid API overload, FDA applies a concurrency cap per API instance:

-   `FDA_MAX_CONCURRENT_FRESH_QUERIES`
-   Exceeded limit returns `429 TooManyFreshQueries`

Guard logic is implemented in [src/lib/utils/utils.js](../../src/lib/utils/utils.js).

Important limitation:

-   This limit is **per instance**, not global across replicas.
-   Multiple API replicas multiply the effective concurrency.

---

## 5. PostgreSQL pooled connections

PostgreSQL operations use connection pooling with reuse and idle cleanup.

-   Pooling logic is implemented in [src/lib/utils/pg.js](../../src/lib/utils/pg.js).
-   A pool is created per target database (multi-tenant model).
-   Clients are acquired with `pool.connect()` and returned with `release()`.
-   Returned connections are **not closed**, but kept idle for reuse.
-   Idle connections are automatically closed after `idleTimeoutMillis`.

Tuning variables:

-   `FDA_PG_POOL_MAX`
-   `FDA_PG_POOL_IDLE_TIMEOUT_MS`
-   `FDA_PG_POOL_CONN_TIMEOUT_MS`

### Behavior summary

-   Connections are reused across operations.
-   `release()` returns the connection to the pool (does not destroy it).
-   The pool grows up to `max` connections under load.
-   Idle connections are eventually cleaned up.

---

## Operational Points to Keep in Mind

### 1. Per-instance limits are not global limits

All concurrency controls are defined per instance:

-   Fresh query limit
-   PostgreSQL pool size
-   DuckDB connection pool

This means total system capacity scales with the number of replicas, but so does total resource consumption.

---

### 2. PostgreSQL capacity can be saturated

Multiple types of operations consume PostgreSQL connections:

-   Fetch jobs (COPY / full table reads)
-   Fresh queries (including streaming cursors)
-   Concurrent API requests

Important considerations:

-   Long-running operations (e.g. large COPY or cursor streaming) hold connections for extended periods.
-   If all pool connections are in use, new requests will wait or fail (depending on timeout).
-   Multiple replicas multiply total concurrent connections against PostgreSQL.

This makes PostgreSQL the primary shared bottleneck in the system.

---

### 3. Pool size must be tuned globally, not per instance

Although pools are defined per instance and per database:

-   Total connections =  
    `num_instances × num_databases × pool_max`

This can lead to unexpected connection exhaustion at the PostgreSQL server level.

Operational recommendation:

-   Size `FDA_PG_POOL_MAX` considering:
    -   number of replicas
    -   number of active tenants (databases)
    -   PostgreSQL max_connections

---

### 4. Pools grow dynamically and are not centrally bounded

Pools are created lazily per database and stored in memory.

Implications:

-   Each new database (tenant) creates a new pool.
-   Pools are not automatically evicted while the process is running.
-   In environments with many tenants, this can lead to unbounded growth in:
    -   number of pools
    -   total potential connections

This is not an issue in low/medium multi-tenant scenarios, but must be considered in large-scale deployments.

---

### 5. Workload types compete for the same resources

The following operations share PostgreSQL capacity:

-   Background refresh jobs (fetchers)
-   Fresh queries (API)
-   Validation queries (during provisioning)

There is currently no global coordination between these workloads.

Implication:

-   High load in one category (e.g. refresh jobs) can impact others (e.g. fresh queries).

---

### 6. API/fetcher ratio impacts system stability

-   Too many fetchers → risk of DB saturation
-   Too many API instances → increased fresh query concurrency
-   Mixed mode can amplify both effects

Scaling strategy should consider:

-   job backlog (Agenda)
-   query traffic patterns
-   PostgreSQL capacity

---

### 7. Graceful shutdown is required

Pools and jobs must be properly closed:

-   `closePgPools()` ensures connections are released
-   Agenda workers must stop cleanly

This is especially important in orchestrated environments (e.g. Kubernetes).

---

## Recommended Monitoring Signals

1. Agenda backlog and job latency.
2. Number of fetch jobs in `fetching` and failed states.
3. Rate of `429 TooManyFreshQueries` responses.
4. PostgreSQL connection usage at server level.
5. Average and p95 duration for:
    - refresh jobs
    - fresh queries
6. Number of active pools (proxy for tenant activity).

These signals give early visibility into scaling pressure before user-facing failures appear.
