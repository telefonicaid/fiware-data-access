# Configuration and Operational Guide

This document describes the configuration options and operational setup for **FIWARE Data Access** (FDA).  
The application is configured primarily through environment variables.

---

## Introduction

`Fiware-data-access` supports configuration via a `.env` file, or in docker service in
[`docker-compose.yml`](../docker/docker-compose.yml).

To set it up:

-   Copy the provided `.env.example` file located in `src/`:

```bash
cp src/.env.example .env
```

and modify the variables in the `.env` file according to your environment.

-   Or modify environment variables in `fda` service inside [`docker-compose.yml`](../docker/docker-compose.yml).

The variables are grouped by category: **Environment**, **PostgreSQL**, and **MongoDB**.

---

## Environment Variables

### Environment

Variables related to the environment of the application:

| Variable          | Optional | Type   | Description                                                                                                           |
| ----------------- | -------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `FDA_NODE_ENV`    | ✓        | string | Level of the node environment. Possible values are `development` and `production`. Value is `development` by default. |
| `FDA_SERVER_PORT` | ✓        | number | Port used by FDA server. Value is `8080` by default.                                                                  |

#### Instance Roles

Variables that define which components of the application are executed by this instance:

| Variable                           | Optional | Type    | Description                                                                                                                                        |
| ---------------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FDA_ROLE_APISERVER`               | ✓        | boolean | If `true`, the instance runs the API server to handle HTTP requests. Default `true`.                                                               |
| `FDA_ROLE_FETCHER`                 | ✓        | boolean | If `true`, the instance runs the fetcher responsible for regenerating and updating FDAs. Default `true`.                                           |
| `FDA_ROLE_SYNCQUERIES`             | ✓        | boolean | If `true`, the API instance accepts `fresh=true` queries and executes them directly against PostgreSQL. Default `false`.                           |
| `FDA_MAX_CONCURRENT_FRESH_QUERIES` | ✓        | number  | Maximum number of concurrent `fresh=true` queries accepted by the API instance. Additional requests return `429 TooManyFreshQueries`. Default `5`. |

> Note: By default, an instance runs both roles (API server and Fetcher). You can disable one to separate
> responsibilities.

> Note: `FDA_ROLE_SYNCQUERIES` is only used by the API server role. It is recommended to enable it only in API instances
> that should allow fresh (non-cached) queries.

### PostgreSQL

Variables related to `PostgreSQL` client:

| Variable                         | Optional | Type   | Description                                                                                            |
| -------------------------------- | -------- | ------ | ------------------------------------------------------------------------------------------------------ |
| `FDA_PG_USER`                    |          | string | User to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                                |
| `FDA_PG_PASSWORD`                |          | string | Password to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                            |
| `FDA_PG_HOST`                    |          | string | Host to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                                |
| `FDA_PG_PORT`                    | ✓        | number | Port to connect to `PostgreSQL` to fetch the data to create the `FDAs`. Value by _default_ **5432**.   |
| `FDA_PG_POOL_MAX`                | ✓        | number | Maximum number of PostgreSQL connections per process and per target database. Default `10`.            |
| `FDA_PG_POOL_IDLE_TIMEOUT_MS`    | ✓        | number | Milliseconds before an idle PostgreSQL pooled connection is closed. Default `10000`.                   |
| `FDA_PG_POOL_CONN_TIMEOUT_MS`    | ✓        | number | Milliseconds to wait when acquiring a PostgreSQL pooled connection before timing out. Default `5000`.  |
| `FDA_PG_POOL_DB_IDLE_TIMEOUT_MS` | ✓        | number | Milliseconds of pool inactivity before closing the whole pool for a target database. Default `300000`. |

### Object bucket-based storage system

Variabes related to the object bucket-based storage system:

| Variable                   | Optional | Type   | Description                                                                                          |
| -------------------------- | -------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `FDA_OBJSTG_USER`          |          | string | User to connect to the object bucket-based storage system.                                           |
| `FDA_OBJSTG_PASSWORD`      |          | string | Password to connect to the object bucket-based storage system.                                       |
| `FDA_OBJSTG_PROTOCOL`      | ✓        | string | Protocol (http or https) to connect to the object bucket-based storage system. Default value `https` |
| `FDA_OBJSTG_ENDPOINT`      |          | string | Endpoint (host and port) to connect to the object bucket-based storage system.                       |
| `FDA_OBJSTG_MAX_POOL_SIZE` | ✓        | number | Max Pool size for connections pool for object storage. Default is 10                                 |

### MongoDB

Variables related to MongoDB:

| Variable        | Optional | Type   | Description                                                                                                                                                                         |
| --------------- | -------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FDA_MONGO_URI` |          | string | Mongodb connection URI to connect to the `MongoDB`. More details in [MongoDb connection URI](https://www.mongodb.com/docs/drivers/node/current/connect/mongoclient/#connection-uri) |

### Logger

| Variable           | Optional | Type   | Description                                                                                      |
| ------------------ | -------- | ------ | ------------------------------------------------------------------------------------------------ |
| `FDA_LOG_LEVEL`    | ✓        | string | Value to define the log level. Possible values `INFO`, `WARN`, `ERROR`, `DEBUG`, default `INFO`. |
| `FDA_LOG_COMP`     | ✓        | string | Name of the component for the log context. Default value `FDA`                                   |
| `FDA_LOG_RES_SIZE` | ✓        | number | Size in characters of the response body showed in the logs. Default value `100`                  |

---

## Example `.env` file

```env
# NODE ENVIRONMENT
FDA_NODE_ENV=development
FDA_SERVER_PORT=8080

# Instance Roles
FDA_ROLE_APISERVER=true
FDA_ROLE_FETCHER=true
FDA_ROLE_SYNCQUERIES=true
FDA_MAX_CONCURRENT_FRESH_QUERIES=5

# POSTGRESQL
FDA_PG_USER=exampleUser
FDA_PG_PASSWORD=examplePass
FDA_PG_HOST=exampleHost
FDA_PG_PORT=5432
FDA_PG_POOL_MAX=10
FDA_PG_POOL_IDLE_TIMEOUT_MS=10000
FDA_PG_POOL_CONN_TIMEOUT_MS=5000
FDA_PG_POOL_DB_IDLE_TIMEOUT_MS=300000

# Object Bucket-Based Storage System
FDA_OBJSTG_USER=exampleUser
FDA_OBJSTG_PASSWORD=examplePass
FDA_OBJSTG_PROTOCOL=http
FDA_OBJSTG_ENDPOINT=endpoint:port
FDA_OBJSTG_MAX_POOL_SIZE=10

# MONGODB
FDA_MONGO_URI=mongodb://exampleUser:examplePassword@endpoint:port

# Logger
FDA_LOG_LEVEL=INFO
FDA_LOG_COMP=FDA
FDA_LOG_RES_SIZE=100
```

---

## Health and Metrics Endpoints

FIWARE Data Access exposes health and telemetry endpoints intended for monitoring, container orchestration systems
(e.g., Kubernetes), load balancers, or uptime checks.

This endpoint does **not** require authentication headers and can be used to verify that the service is running.

### `GET /health`

Health endpoint with liveness and runtime summary.

**Response code**

-   `200 OK` when the service is up.

**Response payload**

```json
{
    "status": "UP",
    "timestamp": "2026-02-16T10:15:30.123Z",
    "uptimeSeconds": 154,
    "process": { "pid": 3210, "nodeVersion": "v24.0.0", "memory": { "rssBytes": 85422080 } },
    "roles": { "apiServer": true, "fetcher": true, "syncQueries": false },
    "traffic": { "totalRequests": 105, "errorRequests": 3, "inFlightRequests": 0, "routesObserved": 8 },
    "fiware": { "requestsWithHeaders": 98, "servicesObserved": 3, "servicePathsObserved": 4 },
    "mongo": {
        "scrapeOk": true,
        "source": "live",
        "fdasTotal": 12,
        "dasTotal": 27,
        "agendaJobsTotal": 7,
        "agendaJobsFailed": 1,
        "agendaJobsLocked": 0
    }
}
```

### `GET /metrics`

OpenMetrics/Prometheus telemetry endpoint.

**Response code**

-   `200 OK` on success.
-   `406 Not Acceptable` when `Accept` does not allow `application/openmetrics-text` or `text/plain`.

**Content negotiation**

-   `application/openmetrics-text` -> `application/openmetrics-text; version=1.0.0; charset=utf-8`
-   missing `Accept`, `text/plain`, or `*/*` -> `text/plain; version=0.0.4; charset=utf-8`

**Example**

```text
# HELP fda_up Service liveness indicator (1=up).
# TYPE fda_up gauge
fda_up 1
# HELP fda_catalog_services_observed Distinct Fiware-Service values seen in traffic.
# TYPE fda_catalog_services_observed gauge
fda_catalog_services_observed 3
# HELP fda_catalog_fdas_total Total number of FDA documents stored in MongoDB.
# TYPE fda_catalog_fdas_total gauge
fda_catalog_fdas_total 12
# HELP fda_jobs_agenda_total Total number of Agenda jobs stored in MongoDB.
# TYPE fda_jobs_agenda_total gauge
fda_jobs_agenda_total 7
# EOF
```

### Metrics Reference Table

The following table summarizes the main metrics exposed by `GET /metrics`, their labels, and typical operational use.

| Metric                                      | Type    | Labels                                                                                   | Purpose                                                           | Typical alert idea                          |
| ------------------------------------------- | ------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------- |
| `fda_up`                                    | gauge   | none                                                                                     | Process liveness indicator (`1` when running).                    | `== 0` for 1-2 scrape intervals             |
| `fda_info`                                  | gauge   | `version`, `node_version`, `env`, `role_api_server`, `role_fetcher`, `role_sync_queries` | Static runtime/build metadata.                                    | no alert; metadata only                     |
| `fda_process_start_time_seconds`            | gauge   | none                                                                                     | Process start time in epoch seconds.                              | no alert; change detection                  |
| `fda_uptime_seconds`                        | gauge   | none                                                                                     | Process uptime.                                                   | frequent resets may indicate instability    |
| `fda_http_server_in_flight_requests`        | gauge   | none                                                                                     | Current in-flight HTTP requests.                                  | sustained high value indicates saturation   |
| `fda_http_server_requests_total`            | counter | `method`, `route`, `status_code`, `status_class`                                         | HTTP usage counter by endpoint and status.                        | sudden drop to zero or unusual distribution |
| `fda_http_server_request_duration_ms_sum`   | counter | `method`, `route`, `status_class`                                                        | Total HTTP latency in ms (sum).                                   | used with `_count` for latency SLI          |
| `fda_http_server_request_duration_ms_count` | counter | `method`, `route`, `status_class`                                                        | Number of timed HTTP requests.                                    | used with `_sum` for latency SLI            |
| `fda_http_server_errors_total`              | counter | `method`, `route`, `status_code`, `status_class`                                         | Error responses (`>= 400`).                                       | error-rate threshold by route               |
| `fda_tenant_requests_total`                 | counter | `fiware_service`, `fiware_service_path`, `method`, `route`, `status_class`               | Tenant/subservice usage breakdown.                                | unusual spikes by tenant or path            |
| `fda_catalog_services_observed`             | gauge   | none                                                                                     | Number of unique `Fiware-Service` values observed in traffic.     | no alert; cardinality watch                 |
| `fda_catalog_service_paths_observed`        | gauge   | none                                                                                     | Number of unique `Fiware-ServicePath` values observed in traffic. | no alert; cardinality watch                 |
| `fda_mongo_scrape_success`                  | gauge   | none                                                                                     | Indicates Mongo snapshot success (`1`) or failure (`0`).          | `== 0` for N consecutive scrapes            |
| `fda_catalog_fdas_total`                    | gauge   | none                                                                                     | Total FDA documents in Mongo (`fdas`).                            | sudden drops / unexpected growth            |
| `fda_catalog_das_total`                     | gauge   | none                                                                                     | Total DA entries in all FDAs.                                     | unexpected drops / growth                   |
| `fda_catalog_fdas_by_status`                | gauge   | `status`                                                                                 | FDAs grouped by execution status (`completed`, `failed`, etc.).   | high `status="failed"` ratio                |
| `fda_catalog_fdas_by_service`               | gauge   | `fiware_service`, `fiware_service_path`                                                  | FDAs by tenant/subservice from persisted catalog.                 | tenant-level drift monitoring               |
| `fda_jobs_agenda_total`                     | gauge   | none                                                                                     | Total jobs in `agendaJobs`.                                       | no alert by itself                          |
| `fda_jobs_agenda_failed_total`              | gauge   | none                                                                                     | Jobs with `failCount > 0`.                                        | increasing value over baseline              |
| `fda_jobs_agenda_locked_total`              | gauge   | none                                                                                     | Jobs currently locked (`lockedAt != null`).                       | prolonged non-zero values                   |
| `fda_jobs_agenda_by_name`                   | gauge   | `job_name`                                                                               | Job count by Agenda job name.                                     | per-job anomaly detection                   |
| `fda_process_resident_memory_bytes`         | gauge   | none                                                                                     | RSS memory usage in bytes.                                        | sustained growth / threshold                |
| `fda_process_heap_total_bytes`              | gauge   | none                                                                                     | Total V8 heap allocation.                                         | no alert by itself                          |
| `fda_process_heap_used_bytes`               | gauge   | none                                                                                     | Used V8 heap memory.                                              | sustained growth / threshold                |

---

## 🧭 Navigation

-   [⬅️ Previous: Architecture](/doc/02_architecture.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Advanced Topics](/doc/05_advanced_topics.md)
