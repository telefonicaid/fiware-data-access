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

---

## 2. Why Asynchronous Processing?

Originally, FDA processing was synchronous. That caused:

-   Long-running HTTP requests
-   Timeout risks
-   Hard-to-track execution state
-   Tight coupling between HTTP and data processing
-   Limited scalability

With the new model:

-   API returns immediately (`202 Accepted`)
-   Execution state is persisted
-   Jobs survive restarts
-   Processing can be distributed
-   Horizontal scaling becomes possible

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

Because jobs are persisted in Mongo, any fetcher instance can process them.

---

## 5. Agenda Integration

We use **agenda** as a Mongo-backed job scheduler.

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

| Field            | Meaning                    |
| ---------------- | -------------------------- |
| `name`           | Job type                   |
| `data`           | Business payload           |
| `nextRunAt`      | Next scheduled execution   |
| `lockedAt`       | Distributed lock timestamp |
| `failCount`      | Retry tracking             |
| `repeatInterval` | Recurring execution config |
| `lastRunAt`      | Last execution start       |
| `lastFinishedAt` | Last execution end         |

Because jobs live in Mongo:

-   They survive crashes
-   They are visible for debugging
-   They allow observability
-   They enable distributed execution

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

Each job:

-   Registers itself in Agenda
-   Encapsulates business logic
-   Updates FDA status in Mongo
-   Handles errors & retries

Typical lifecycle inside a job:

1. `status = fetching`
2. Fetch from PostgreSQL
3. Transform data
4. Upload to object storage
5. Update status → `completed` or `failed`

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

## 9. MongoDB as Execution State Backbone

MongoDB now stores:

-   FDA configuration
-   Operational fields:

    -   `status`
    -   `progress`
    -   `lastFetch`

-   Agenda job metadata

This guarantees:

-   Crash recovery
-   State traceability
-   Idempotent execution
-   Observability
-   Horizontal scaling safety

Mongo is effectively the **single source of truth** for:

-   What should run
-   What is running
-   What failed
-   What finished

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
2. Agenda job scheduled
3. HTTP `202 Accepted` returned
4. Fetcher picks up job
5. Mongo updated through lifecycle

Status transitions are defined in:

`Advanced Topics → FDA Execution Lifecycle`

---

## 12. Failure Handling & Recovery

If `processFDAAsync()` throws:

-   Agenda marks the job as failed
-   `failCount` is incremented
-   `failedAt` and `failReason` are stored

There is currently:

-   No custom retry logic
-   No backoff strategy
-   No dead-letter handling

Failure behavior relies entirely on Agenda’s built-in mechanics.
