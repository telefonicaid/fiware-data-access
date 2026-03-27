# Async Processing & Job Architecture

## 1. Introduction

`FDA` uses an asynchronous processing model to execute data fetch and transformation operations. Instead of performing
heavy operations inside the HTTP request lifecycle, work is delegated to background jobs managed by **agenda** and
persisted in **MongoDB**.

This document explains:

-   Why background jobs were introduced
-   How Agenda is integrated
-   API / Fetcher runtime modes
-   How jobs are persisted in Mongo
-   How execution state is tracked
-   Cron and recurring execution capabilities
-   Backoff strategies and failure handling

---

## 2. Why Asynchronous Processing?

Originally, FDA processing was synchronous. That caused:

-   Long-running HTTP requests
-   Timeout risks
-   Hard-to-track execution state
-   Tight coupling between HTTP and data processing
-   Limited scalability

With the current asynchronous model:

-   API returns immediately (`202 Accepted`)
-   Execution state is persisted in Mongo
-   Jobs survive restarts
-   Processing can be distributed
-   Horizontal scaling becomes possible across nodes

---

## 3. High-Level Architecture

```
Client → API → Mongo (FDA metadata)
                ↓
        Agenda Job (persisted)
                ↓
            Fetcher Worker
                ↓
 PostgreSQL → Transform → Object Storage
```

### Responsibilities

| Component | Responsibility                          |
| --------- | --------------------------------------- |
| API       | Validate input & persist metadata       |
| MongoDB   | Store FDA state + Agenda jobs           |
| Agenda    | Schedule & coordinate background jobs   |
| Fetcher   | Execute data retrieval & transformation |

---

## 4. Runtime Modes (API / Fetcher / Mixed)

FDA supports **role-based execution**, controlled via environment variables:

```ts
FDA_ROLE_APISERVER: {
  type: 'boolean',
  default: true,
},
FDA_ROLE_FETCHER: {
  type: 'boolean',
  default: true,
},
FDA_ROLE_SYNCQUERIES: {
  type: 'boolean',
  default: false,
},
```

Startup logic:

```ts
if (config.roles.apiServer) {
  app.listen(PORT, ...)
}

if (config.roles.fetcher) {
  startFetcher()
}
```

### Supported Deployment Modes

| Mode            | API | Fetcher | Use Case                     |
| --------------- | --- | ------- | ---------------------------- |
| API-only        | ✅  | ❌      | Edge service, request intake |
| Fetcher-only    | ❌  | ✅      | Dedicated worker node        |
| Mixed (default) | ✅  | ✅      | Simple deployments           |

`FDA_ROLE_SYNCQUERIES` is an additional API capability flag (not a standalone worker role): when enabled, the API
accepts `fresh=true` in `GET /{scope}/fdas/{fdaId}/das/{daId}/data` to execute DA queries directly on PostgreSQL.

To protect API workers from overload, fresh-query concurrency is bounded by `FDA_MAX_CONCURRENT_FRESH_QUERIES` (default
`5`). Extra requests return `429 TooManyFreshQueries`.

### Why This Matters

This enables:

-   Clean horizontal scaling
-   Worker autoscaling
-   Independent lifecycle management
-   Kubernetes-native deployment strategies

For example:

-   2 API replicas
-   5 Fetcher replicas
-   All sharing the same MongoDB

Because jobs are persisted in Mongo, any fetcher instance can pick them up safely.

---

## 5. Agenda Integration

We use **Agenda** as a Mongo-backed job scheduler.

### Initialization

Agenda:

-   Connects to MongoDB
-   Uses a dedicated collection: `agendaJobs`
-   Registers job definitions
-   Starts polling for due jobs

### Characteristics

-   Jobs are persisted
-   Survive restarts
-   Distributed locking
-   Retry support
-   Concurrency control
-   Supports backoff strategies

Reference:
[Agenda – Defining Job Processors](https://github.com/agenda/agenda?tab=readme-ov-file#defining-job-processors)

---

## 6. Job Persistence in MongoDB

Jobs are stored in:

Database:

```
fiware-data-access
```

Collection:

```
agendaJobs
```

Example document:

```json
{
    "_id": "69a5a54d58c119359dbfa615",
    "name": "refresh-fda",
    "type": "single",
    "data": {
        "fdaId": "fda_alarms",
        "query": "SELECT * FROM public.alarms",
        "service": "my-bucket",
        "lastModifiedBy": null
    },
    "nextRunAt": "2026-03-02T14:58:20.213Z",
    "priority": 0,
    "repeatInterval": "1 minute",
    "lockedAt": null,
    "failCount": null,
    "failReason": null,
    "failedAt": null,
    "lastFinishedAt": "2026-03-02T14:57:21.582Z",
    "lastRunAt": "2026-03-02T14:57:20.213Z"
}
```

### Important Fields

| Field            | Meaning                           |
| ---------------- | --------------------------------- |
| `name`           | Job type                          |
| `data`           | Business payload                  |
| `nextRunAt`      | Next scheduled execution          |
| `lockedAt`       | Distributed lock timestamp        |
| `failCount`      | Retry tracking                    |
| `failReason`     | Reason for failure                |
| `repeatInterval` | Recurring execution configuration |
| `lastRunAt`      | Last execution start              |
| `lastFinishedAt` | Last execution end                |

> **Key Implementation Detail:** Agenda ensures distributed locking (`lockedAt` + `lockLifetime`). No need to implement
> manual locks in Mongo, avoiding blocked or stale jobs.

---

## 7. Job Definitions Structure

Jobs are defined in:

```
src/fetcher.js
```

At this time:

```js
agenda.define('refresh-fda', async (job) => {
    const { fdaId, query, service } = job.attrs.data;
    await processFDAAsync(fdaId, query, service);
});
```

Notes:

-   Each job registers itself in Agenda
-   Encapsulates business logic
-   Updates FDA status in Mongo (`fetching` → `completed` / `failed`)
-   Handles errors & retries automatically via Agenda

> **Tip:** Use `async` or the `done` callback correctly to ensure job unlock. See
> [Agenda – Job Processors](https://github.com/agenda/agenda?tab=readme-ov-file#defining-job-processors).

---

## 8. Cron & Recurring Jobs

Agenda supports:

-   Human intervals (`1 minute`)
-   Cron expressions
-   One-off jobs
-   Repeating jobs

Examples:

```
"1 minute"
"5 minutes"
"0 * * * *"        // every hour
"0 0 * * *"        // daily at midnight
```

This enables:

-   Periodic FDA refresh
-   Scheduled batch jobs
-   Event-driven or time-driven workflows

Recurring configuration is stored directly in:

```
repeatInterval
```

This means:

-   Recurrence is persisted
-   Survives restarts
-   Can be modified dynamically

---

## 9. Failure Handling & Backoff Strategies

Agenda supports automatic retry with configurable backoff:

-   **Constant:** Same delay every retry
-   **Linear:** Delay increases by a fixed amount
-   **Exponential:** Delay multiplies by factor
-   **Preset strategies:** `aggressive()`, `standard()`, `relaxed()`
-   **Custom functions:** Implement your own delay sequence
-   **Conditional retry:** Only retry for specific errors using `when()`

Example:

```js
agenda.define(
    'send-email',
    async (job) => {
        await sendEmail(job.attrs.data);
    },
    {
        backoff: agenda.backoffStrategies.standard(),
    },
);
```

> **Important:** `failCount` tracks attempts. Retry logic is per-job, not global. Repeating jobs (`every()`) will retry
> immediately if failed, then continue normal schedule.

See
[Agenda – Automatic Retry with Backoff](https://github.com/agenda/agenda?tab=readme-ov-file#automatic-retry-with-backoff).

---

## 10. API vs Fetcher Separation

### API Layer

Responsible for:

-   Validating request
-   Persisting FDA metadata
-   Scheduling Agenda job
-   Returning immediate HTTP response

It does NOT:

-   Fetch data
-   Transform data
-   Upload data

---

### Fetcher Layer (Worker)

Responsible for:

-   Polling Agenda
-   Executing jobs
-   Updating execution state
-   Handling retries
-   Managing concurrency

This separation:

-   Keeps controllers lightweight
-   Decouples HTTP from processing
-   Improves testability
-   Enables worker autoscaling
-   Enables API-only deployments

---

## 11. Execution Flow

When:

```
POST /fdas
PUT /fdas/:fdaId
```

Flow:

1. FDA metadata saved in Mongo
2. Agenda job scheduled (`agenda.now()` for immediate fetch, `agenda.every()` for recurring)
3. HTTP `202 Accepted` returned
4. Fetcher picks up job
5. Mongo updated throughout lifecycle

### Query availability during first fetch

To keep early DA validation without waiting for the first asynchronous job completion:

-   FDA provisioning creates the canonical `${fdaId}.parquet` synchronously from a one-row snapshot.
-   DA create/update validates compatibility using DuckDB `prepare` against that parquet.
-   Query execution is blocked until the first successful fetch has completed (`lastFetch` exists), returning
    `409 FDAUnavailable` beforehand.
-   After the first successful fetch, queries can still run during later `fetching` states using the last available
    parquet snapshot.

---

## 12. Observability & State Tracking

MongoDB stores:

-   FDA metadata & operational fields (`status`, `progress`, `lastFetch`)
-   Agenda job metadata (`lockedAt`, `failCount`, `nextRunAt`)

This guarantees:

-   Crash recovery
-   State traceability
-   Idempotent execution
-   Observability across multiple nodes
-   Horizontal scaling safety

MongoDB is effectively the **single source of truth** for what should run, what is running, and what failed.

---

## Summary of Key Implementation Details

-   `processFDAAsync()` handles status updates (`fetching` → `completed` / `failed`)
-   Agenda locks jobs automatically; manual `isRunning` flags are unnecessary
-   Recurring jobs and cron schedules are persisted in `repeatInterval`
-   Backoff strategies handle retries in a controlled manner
-   Multiple node instances can process the same queue safely using `lockLifetime`

## References

-   [Agenda GitHub Repository](https://github.com/agenda/agenda)
