# Fiware Data Access API Reference

## Table of Contents

-   [Introduction](#introduction)
-   [Error Responses](#error-responses)
-   [API Routes](#api-routes)
    -   [Health Endpoint](#health-endpoint)
        -   [Health Check `GET /health`](#health-check-get-health)
    -   [FDA payload datamodel](#fda-payload-datamodel)
    -   [FDAs operations](#fdas-operations)
        -   [List FDAs](#list-fdas-get-visibilityfdas)
        -   [Create FDA](#create-fda-post-visibilityfdas)
        -   [Get FDA](#get-fda-get-visibilityfdasfdaid)
        -   [Regenerate FDA](#regenerate-fda-put-visibilityfdasfdaid)
        -   [Delete FDA](#delete-fda-delete-visibilityfdasfdaid)
    -   [DA payload datamodel](#da-payload-datamodel)
    -   [DAs operations](#das-operations)
        -   [List DAs](#list-das-get-visibilityfdasfdaiddas)
        -   [Create DA](#create-da-post-visibilityfdasfdaiddas)
        -   [Get DA](#get-da-get-visibilityfdasfdaiddasdaid)
        -   [Update DA](#update-da-put-visibilityfdasfdaiddasdaid)
        -   [Delete DA](#delete-da-delete-visibilityfdasfdaiddasdaid)
    -   [Data operations](#data-operations)
        -   [Data query](#data-query-get-visibilityfdasfdaiddasdaiddata)
        -   [Query (Pentaho CDA legacy support)](#query-post-plugincdaapidoquery-pentaho-cda-legacy-support)
-   [Navigation](#-navigation)

## Introduction

This document describes the API used by the FIWARE Data Access component.

This API is inspired in RESTful principles and we have two different resource types:

-   **fdas**: corresponding to a "raw" fda, fetched from DB and corresponding to a Parquet file in the object
    bucket-based storage system.
-   **data accesses (das)**: corresponding to particular query over a fda

There is a dependency relationship between the two types, as the _das_ belongs to a given _fda_.

The datamodel associated to this API (i.e. how fdas and das are modeled in MongoDB) is out of the scope of this
document.

Aditionally all the API routes described in this document are included in a [Postman collection](./postman/README.md) to
ease the use of the app.

## Error responses

FDA uses HTTP status codes and specific error codes to communicate the result of API operations. When an error occurs,
the API returns a JSON response with detailed information about what went wrong.

### Error response format

All error responses follow this structure:

```json
{
    "error": "ErrorCode",
    "description": "Detailed description of the error"
}
```

**Fields:**

-   `error` (required, string): A machine-readable error code that identifies the type of error.
-   `description` (optional, string): Additional human-readable information about the error. The exact wording may vary
    between FDA versions.

### HTTP status codes

| Code | Status                | Error Code             | Cause                                                                                                                                                                             |
| ---- | --------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 400  | Bad Request           | `BadRequest`           | Missing or invalid values in request body, headers, or query parameters. `Fiware-Service`, `Fiware-ServicePath`, and `visibility` (path segment) are required for all operations. |
| 400  | Bad Request           | `BadRequest`           | An unsupported `outputType` value was provided. Allowed values: `json`, `csv`, `xls`.                                                                                             |
| 400  | Bad Request           | `InvalidVisibility`    | The `visibility` path segment is not one of the allowed values (`public`, `private`).                                                                                             |
| 400  | Bad Request           | `InvalidServicePath`   | The `Fiware-ServicePath` header value is not a valid absolute path (e.g. `/` or `/servicePath/site`).                                                                             |
| 400  | Bad Request           | `InvalidQueryParam`    | Some of the params in the request don't comply with the [params](#params) array restrictions.                                                                                     |
| 403  | Forbidden             | `VisibilityMismatch`   | The FDA exists but was created under a different `visibility`. Cannot access a private FDA through a public route and vice-versa.                                                 |
| 400  | Bad Request           | `PartitionError`       | Some of the params related to the creation of the parquet partition don't comply with the [object storage configuration](#object-storage-configuration-objstgconf) requirements.  |
| 400  | Bad Request           | `CleaningError`        | Trying to remove a non partitioned FDA or incorrect value in the [delete interval key](#refresh-policy-object).                                                                   |
| 404  | Not Found             | `FDANotFound`          | The requested FDA was not found.                                                                                                                                                  |
| 404  | Not Found             | `DaNotFound`           | The requested Data Access (DA) was not found.                                                                                                                                     |
| 409  | Conflict              | `DuplicatedKey`        | The resource already exists in the database. Attempting to create a duplicate resource.                                                                                           |
| 429  | Too Many Requests     | `TooManyFreshQueries`  | The number of concurrent `fresh=true` queries exceeded `FDA_MAX_CONCURRENT_FRESH_QUERIES`.                                                                                        |
| 409  | Conflict              | `FDAUnavailable`       | FDA `exampleId` is not queryable yet because the first fetch has not completed.                                                                                                   |
| 500  | Internal Server Error | `S3ServerError`        | An error occurred in the S3 object storage component.                                                                                                                             |
| 500  | Internal Server Error | `DuckDBServerError`    | An error occurred in the DuckDB component.                                                                                                                                        |
| 500  | Internal Server Error | `MongoDBServerError`   | An error occurred in the MongoDB component.                                                                                                                                       |
| 503  | Service Unavailable   | `UploadError`          | Connection error with the PostgreSQL database component.                                                                                                                          |
| 503  | Service Unavailable   | `SyncQueriesDisabled`  | A request was sent with `fresh=true` but the API instance is running with `FDA_ROLE_SYNCQUERIES=false`.                                                                           |
| 503  | Service Unavailable   | `MongoConnectionError` | Connection error with the MongoDB component.                                                                                                                                      |

### Common error scenarios

#### Missing required headers

When required headers (`Fiware-Service` or `Fiware-ServicePath`) are not provided:

**Request:**

```bash
curl -i http://localhost:8080/public/fdas
```

**Response (400):**

```json
{
    "error": "BadRequest",
    "description": "Missing params in the request"
}
```

#### Missing required query parameters

When required query parameters are missing from the request:

**Request:**

```bash
curl -i http://localhost:8080/public/fdas/fda1/das/da1/data \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

**Response (400):**

```json
{
    "error": "BadRequest",
    "description": "Missing params in the request"
}
```

#### Missing required body fields

When required fields are missing from the request payload:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda01"
  }'
```

**Response (400):**

```json
{
    "error": "BadRequest",
    "description": "Missing params in the request"
}
```

#### Adding invalid body fields

If any field not explicitly allowed for the operation (including read-only or operational fields) is included in the
request body, the request will be rejected with:

-   **400 BadRequest**
-   `Invalid fields in request body, check your request`

**Request:**

```bash
curl -i -X PUT http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "query": "SELECT * LIMIT 5",
    "description": "Invalid DA",
    "foo": "bar"
  }'
```

**Response (400):**

```json
{
    "error": "BadRequest",
    "description": "Invalid fields in request body, check your request"
}
```

#### Resource not found (FDA)

When requesting an FDA that doesn't exist:

**Request:**

```bash
curl -i http://localhost:8080/public/fdas/nonexistent \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

**Response (404):**

```json
{
    "error": "FDANotFound",
    "description": "FDA nonexistent not found in service my-bucket"
}
```

#### Resource not found (DA)

When requesting a Data Access that doesn't exist:

**Request:**

```bash
curl -i http://localhost:8080/public/fdas/fda_alarms/das/nonexistent-da \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

**Response (404):**

```json
{
    "error": "DaNotFound",
    "description": "DA nonexistent-da not found in FDA fda_alarms and service my-bucket."
}
```

#### Duplicate resource

When attempting to create a resource that already exists:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda_alarms",
    "query": "SELECT * FROM public.alarms",
    "description": "Intento duplicado"
  }'
```

**Response (409):**

```json
{
    "error": "DuplicatedKey",
    {"description":"FDA with id fda_alarms and my-bucket already exists: MongoServerError: E11000 duplicate key error collection: fiware-data-access.fdas index: fdaId_1_service_1 dup key: { fdaId: \"fda_alarms\", service: \"my-bucket\" }"}
}
```

#### Database connection error

When a connection error occurs with a backend service:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda_test",
    "query": "SELECT * FROM public.nonexistent_table",
    "description": "Test"
  }'
```

**Response (503) - Bucket does not exist:**

```json
{
    "error": "UploadError",
    "description": "Error uploading FDA to object storage: The specified bucket does not exist"
}
```

**Response (503) - Invalid SQL column:**

```json
{
    "error": "UploadError",
    "description": "Error uploading FDA to object storage: column \"severity\" does not exist"
}
```

#### Internal server error

When an unexpected error occurs during request processing:

**Response (500):**

```json
{
    "error": "S3ServerError",
    "description": "Error accessing S3 object storage"
}
```

#### Configuration errors

**SSL/TLS Protocol Error** - When MinIO is configured to use HTTPS but the client uses HTTP:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda_alarms",
    "query": "SELECT * FROM public.alarms"
  }'
```

**Response (503):**

```json
{
    "error": "UploadError",
    "description": "Error uploading FDA to object storage: write EPROTO 00AD771738720000:error:0A0000C6:SSL routines:tls_get_more_records:packet length too long:../deps/openssl/openssl/ssl/record/methods/tls_common.c:662:\n"
}
```

**Solution:** Ensure the `FDA_OBJSTG_PROTOCOL` environment variable is set to `http` when using local MinIO.

### Important notes on error handling

1. **HTTP Status Code**: Always check the HTTP status code first to determine if the request was successful.

2. **Error codes are machine-readable**: The `error` field contains a standardized code that can be used for
   programmatic error handling.

3. **Descriptions vary**: The `description` field provides human-readable details that may vary between API versions. Do
   not rely on exact wording for programmatic logic.

4. **Fiware-Service header is mandatory**: All requests must include this header. Missing it will always result in a
   `400 BadRequest` error.

5. **SQL injection protection**: Column names with special characters (like `__SEVERITY__`) must be quoted in SQL
   queries to avoid errors.

6. **Bucket existence**: The S3 bucket name must match the `Fiware-Service` header value and must exist before creating
   FDAs.

## API Routes

### Health Endpoint

This endpoint allows checking whether the FIWARE Data Access service is running.

It does not require the `Fiware-Service` header and is intended for monitoring purposes.

---

#### Health Check `GET /health`

Returns the operational status of the service plus runtime and traffic context useful for operations.

**Request headers**

None required.

**Response code**

-   `200 OK` — Service is running.

**Response payload**

```json
{
    "status": "UP",
    "timestamp": "2026-02-16T10:15:30.123Z",
    "uptimeSeconds": 154,
    "process": {
        "pid": 3210,
        "nodeVersion": "v24.0.0",
        "memory": {
            "rssBytes": 85422080,
            "heapTotalBytes": 33230848,
            "heapUsedBytes": 19459968
        }
    },
    "roles": {
        "apiServer": true,
        "fetcher": true,
        "syncQueries": false
    },
    "traffic": {
        "totalRequests": 105,
        "errorRequests": 3,
        "inFlightRequests": 0,
        "routesObserved": 8
    },
    "fiware": {
        "requestsWithHeaders": 98,
        "servicesObserved": 3,
        "servicePathsObserved": 4
    },
    "mongo": {
        "scrapeOk": true,
        "source": "live",
        "lastSuccessTimestamp": "2026-04-06T08:52:58.595Z",
        "fdasTotal": 2,
        "dasTotal": 1,
        "agendaJobsTotal": 0,
        "agendaJobsFailed": 0,
        "agendaJobsLocked": 0
    }
}
```

### Metrics Endpoint

The service exposes a telemetry endpoint compatible with Prometheus text format and OpenMetrics content negotiation.

#### Retrieve metrics `GET /metrics`

**Request headers**

-   Optional `Accept` header.

**Response code**

-   `200 OK` if successful.
-   `406 Not Acceptable` if the `Accept` header does not include a supported format.

**Response content-type**

-   If `Accept` contains `application/openmetrics-text`, response content-type is
    `application/openmetrics-text; version=1.0.0; charset=utf-8`.
-   If `Accept` is missing or supports `text/plain` (explicitly or through `*/*`), response content-type is
    `text/plain; version=0.0.4; charset=utf-8`.

**Response payload**

OpenMetrics-compatible plain text, including HELP/TYPE metadata, e.g.:

```text
# HELP fda_up Service liveness indicator (1=up).
# TYPE fda_up gauge
fda_up 1
# HELP fda_http_server_requests_total Total HTTP requests served.
# TYPE fda_http_server_requests_total counter
fda_http_server_requests_total{method="GET",route="/health",status_class="2xx",status_code="200"} 4
# HELP fda_tenant_requests_total Total HTTP requests carrying FIWARE tenant headers.
# TYPE fda_tenant_requests_total counter
fda_tenant_requests_total{fiware_service="my-bucket",fiware_service_path="/",method="GET",route="/:visibility/fdas",status_class="2xx"} 8
# HELP fda_catalog_fdas_by_service Number of FDA documents by fiware service and servicePath.
# TYPE fda_catalog_fdas_by_service gauge
fda_catalog_fdas_by_service{fiware_service="my-bucket",fiware_service_path="/"} 12
# HELP fda_jobs_agenda_total Total number of Agenda jobs stored in MongoDB.
# TYPE fda_jobs_agenda_total gauge
fda_jobs_agenda_total 7
...
# EOF
```

### FDA payload datamodel

A FDA is represented by a JSON object with the following fields:

| Parameter                                                | Optional | Type   | Description                                                                                                                       |
| -------------------------------------------------------- | -------- | ------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `id`                                                     |          | string | FDA unique identifier                                                                                                             |
| `description`                                            | ✓        | string | A free text used by the client to describe the FDA                                                                                |
| `query`                                                  |          | string | Base `postgreSQL` query to create the file in the bucket-based storage system                                                     |
| `refreshPolicy`                                          | ✓        | object | Optional policy for automatic refresh.                                                                                            |
| [`objStgConf`](#object-storage-configuration-objstgconf) | ✓        | object | Various options to configure the FDA uploaded in the object storage app.                                                          |
| `timeColumn`                                             | ✓        | string | Required with `refreshPolicy` of type `window` and `partition`. Column in the table indicating when the data was received (date). |

#### Refresh Policy object

Defines how and when the FDA should be automatically refreshed.

| Field            | Optional | Type   | Description                                                                                                                                                                                                                                     |
| ---------------- | -------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`           |          | string | Refresh strategy. One of: `none`, `interval`, `cron`, `window`.                                                                                                                                                                                 |
| `value`          | ✓        | string | Required if `type` is `interval`, `cron` or `window`. With type `interval` and `cron` it represents a human interval (e.g. `1 hour`) or a cron expression. With type `window` it can take the values `hourly`, `daily`, `weekly` and `monthly`. |
| `deleteInterval` | ✓        | string | Represents a human interval (e.g. `1 hour`) or a cron expression.                                                                                                                                                                               |
| `windowSize`     | ✓        | string | Required with `deleteInterval`. Temporal interval of data we are gonna keep in storage (e.g. only the data of the last month). Possible values: `day`, `week`, `month` and `year`                                                               |

##### Semantics

-   `none` (default): No automatic refresh is scheduled.
-   `interval`: Uses Agenda [human interval](https://github.com/agenda/human-interval) format (e.g. `5 minutes`,
    `1 hour`).
-   `cron`: Uses a cron expression (e.g. `0 * * * *`).
-   `window`: Uses the values `hourly`, `daily`, `weekly` and `monthly`. When refreshing the `FDA` it retrieves only the
    data of the interval indicated in the value (e.g. with the value `weekly` it retrieves each week the data of the
    last week).

If omitted, the default policy is:

```json
{ "type": "none" }
```

##### Object storage configuration (objstgconf)

This object configures certain aspects of the object storage app when uploading an FDA. The possible keys are:

| Parameter     | Optional | Type    | Description                                                                                                               |
| ------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------- |
| `partition`   | ✓        | string  | Tells how the FDA data should be partitioned in the object storage app. Possibe values `day`, `week`, `month` and `year`. |
| `compression` | ✓        | boolean | Tells if the FDA parquet file should be compressed (using `ZSTD` compression) or not.                                     |

#### Operational fields (read-only)

These fields are **provided in responses** but **cannot be included or modified** in POST or PUT requests:

| Parameter   | Optional | Type   | Description                                                                                   |
| ----------- | -------- | ------ | --------------------------------------------------------------------------------------------- |
| `status`    |          | string | Current FDA execution status (`fetching`, `transforming`, `uploading`, `completed`, `failed`) |
| `progress`  |          | number | Execution progress percentage (0–100)                                                         |
| `lastFetch` |          | string | Timestamp of the last fetch (ISO date format)                                                 |

> Note: Including operational fields like `progress` or `status` in POST/PUT requests is ignored by the server. Requests
> including these fields are rejected with `400 BadRequest`.

### FDAs operations

#### List FDAs `GET /{visibility}/fdas`

Returns a list of all the FDAs for that `service`, `servicePath` and visibility.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Filters results to exact match.      | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array containing one object per FDA. Each FDA follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

Each element includes `id` and excludes context/internal fields (`_id`, `fdaId`, `service`, `visibility`, `servicePath`,
etc.) because those are already provided by request scope.

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/public/fdas \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
[
    {
        "id": "fda_alarms",
        "query": "SELECT * FROM public.alarms",
        "das": {},
        "status": "completed",
        "progress": 100,
        "lastFetch": "2026-02-19T07:38:21.263Z",
        "refreshPolicy": { "type": "interval", "value": "1 hour" },
        "description": "FDA de alarmas del sistema"
    }
]
```

#### Create FDA `POST /{visibility}/fdas`

Creates a new FDA

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example            |
| -------------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`       |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`        |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Stored and exact-matched on access.  | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda_alarms",
    "query": "SELECT * FROM public.alarms",
    "description": "FDA de alarmas del sistema",
    "refreshPolicy": { "type": "interval", "value": "1 hour" }
  }'
```

_**Response code**_

-   Successful operation uses 202 Accepted (asynchronous processing)
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None

_**Response payload**_

```json
{
    "id": "fda_alarms",
    "status": "pending"
}
```

_**Example Response:**_

```
HTTP/1.1 201 Created
X-Powered-By: Express
Content-Type: text/plain; charset=utf-8
Content-Length: 7

Created
```

#### Get FDA `GET /{visibility}/fdas/{fdaId}`

Returns the FDA requested.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

A JSON object containing the FDA data, excluding redundant context/internal fields (`_id`, `fdaId`, `service`,
`visibility`, `servicePath`, etc.).

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/public/fdas/fda_alarms \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
{
    "query": "SELECT * FROM public.alarms",
    "das": {},
    "status": "completed",
    "progress": 100,
    "lastFetch": "2026-02-19T07:38:21.263Z",
    "refreshPolicy": { "type": "interval", "value": "1 hour" },
    "description": "FDA de alarmas del sistema"
}
```

#### Regenerate FDA `PUT /{visibility}/fdas/{fdaId}`

Regenerate the FDA, fetching again the source table from DB. If the FDA is currently being processed, the operation
returns `409 Conflict`.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

This endpoint does not accept a request body.

If a body is provided, the API will return:

-   **400 BadRequest**
-   `PUT /{visibility}/fdas/{fdaId} does not accept a request body`

_**Response code**_

-   Successful operation uses 202 Accepted (asynchronous regeneration)
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None

_**Response payload**_

```json
{
    "id": "fda_alarms",
    "status": "pending"
}
```

#### Delete FDA `DELETE /{visibility}/fdas/{fdaId}`

Delete FDA. Note that deleting a FDA deletes in cascade all the DAs belonging to it.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 204 No Content
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None

_**Response payload**_

None

### DA payload datamodel

A DA is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                                                     |
| ------------- | -------- | ------ | ------------------------------------------------------------------------------- |
| `id`          | (\*)     | string | DA identifier, unique within the associated FDA.                                |
| `description` | ✓        | string | A free text used by the client to describe the DA                               |
| `query`       |          | string | Query string, without **FROM**, clause to run over the FDA when invoking the DA |
| `params`      | ✓        | array  | Array of [param objects](#params) to control param values.                      |

(\*) The `id` field is mandatory when creating a DA (`POST`) and must not be included when updating a DA (`PUT`).

#### Params

Each object in the array `params` can have the following keys:

| Parameter  | Optional | Type    | Description                                                                                                                                                                                              |
| ---------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`     |          | string  | Name of the param to control.                                                                                                                                                                            |
| `type`     |          | string  | Type of the param to enforce. Possible values: _Number_, _Boolean_, _Text_ and _DateTime_.                                                                                                               |
| `required` | ✓        | boolean | Tell if the param must be provided by the user. Default value _false_.                                                                                                                                   |
| `default`  | ✓        | string  | Provide a default value for the param in case it isn't provided.                                                                                                                                         |
| `range`    | ✓        | array   | Array with the minimun and maximun value (`Number`) a param can take. The array should be consistent (only two elements and the first one lesser than the second), otherwise an error will be responsed. |
| `enum`     | ✓        | array   | Array with all the possible values (`Number` or `Text`) a param can take.                                                                                                                                |

Example array:

```
[
    {
        "name": "timeinstant",
        "type": "DateTime",
        "default": "2020-08-17T18:25:28.332+01:00"
    },
    {
        "name": "animalname",
        "type": "Text",
        "enum": ["TUNA", "Bandolera"]
    },
    {
        "name": "counted",
        "type": "Boolean",
        "default": true
    },
    {
        "name": "activity",
        "type": "Number",
        "required": true,
        "range": [10, 14]
    }
]
```

### DAs operations

#### List DAs `GET /{visibility}/fdas/{fdaId}/das`

Returns a list of all the DAs associated to a given FDA.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array containing one object per DA. Each DA follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/public/fdas/fda_alarms/das \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
[
    {
        "id": "da_all_alarms",
        "description": "Todas las alarmas",
        "query": "SELECT * LIMIT 10"
    },
    {
        "id": "da_filter_by_name",
        "description": "Filtrar alarmas por nombre",
        "query": "SELECT entityID, __NAME__, __SEVERITY__ WHERE __NAME__ LIKE $pattern ORDER BY entityID"
    }
]
```

#### Create DA `POST /{visibility}/fdas/{fdaId}/das`

Create a new DA on a given FDA.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example            |
| -------------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`       |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`        |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas/fda_alarms/das \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "da_all_alarms",
    "description": "Todas las alarmas",
    "query": "SELECT * LIMIT 10"
  }'
```

_**Example Response:**_

```
HTTP/1.1 201 Created
X-Powered-By: Express
Content-Type: text/plain; charset=utf-8
Content-Length: 7

Created
```

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the DA (I.E : `/public/fdas/fda01/das/da01`)
    when the creation succeeds (Response code 201).

_**Response payload**_

None

#### Get DA `GET /{visibility}/fdas/{fdaId}/das/{daId}`

Return the DA requested.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |
| `daId`       |          | Id of the DA                                               | `da1`    |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
{
    "description": "Todas las alarmas",
    "query": "SELECT * LIMIT 10",
    "id": "da_all_alarms"
}
```

#### Update DA `PUT /{visibility}/fdas/{fdaId}/das/{daId}`

Update an existing DA.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |
| `daId`       |          | Id of the DA                                               | `da1`    |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example            |
| -------------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`       |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`        |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section). The DA is updated with that content.

_**Example Request:**_

```bash
curl -i -X PUT http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "description": "Todas las alarmas (actualizado)",
    "query": "SELECT * LIMIT 20"
  }'
```

_**Response code**_

-   Successful operation uses 204 No Content
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None

_**Response payload**_

None

#### Delete DA `DELETE /{visibility}/fdas/{fdaId}/das/{daId}`

Delete DA.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |
| `daId`       |          | Id of the DA                                               | `da1`    |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 204 No Content
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None

_**Response payload**_

None

### Data operations

#### Data query `GET /{visibility}/fdas/{fdaId}/das/{daId}/data`

Runs a stored parameterized query for the selected DA. The request path declares the access visibility and the query
string carries DA parameters.

_**Request path parameters**_

| Parameter    | Optional | Description                                                          | Example  |
| ------------ | -------- | -------------------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private`           | `public` |
| `fdaId`      |          | Id of the `fda`. Must be unique in combination with `Fiware-Service` | `fda1`   |
| `daId`       |          | Id of the `da`. Must be unique inside each `fda`                     | `da1`    |

_**Request query parameters**_

| Parameter    | Optional | Description                                                                                                                                              | Example |
| ------------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `outputType` | ✓        | Format of the returned results. **Default:** `json`. Allowed values: `json`, `csv`, `xls`.                                                               | `csv`   |
| `fresh`      | ✓        | If `true`, executes the DA directly against PostgreSQL instead of the cached Parquet snapshot. Requires `FDA_ROLE_SYNCQUERIES=true` in the API instance. | `true`  |

Additionally, the DA-specific parameters must be included in the query string together with the previous ones.

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`    |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   If the FDA has not completed its first fetch yet, operation uses 409 Conflict with `FDAUnavailable`
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Behavior note**_

-   DA creation is allowed while an FDA is still processing the first fetch, using an internal one-row synchronous
    parquet snapshot to validate DA query compatibility.
-   Query execution is blocked until the first successful fetch is completed (`lastFetch` available).
-   After that first completion, query execution is allowed even if a later regeneration is in progress, returning the
    last available parquet snapshot.

_**Response headers**_

| `outputType` value | `Content-Type`                                                      | `Content-Disposition`                 |
| ------------------ | ------------------------------------------------------------------- | ------------------------------------- |
| `json` (default)   | `application/json`                                                  | —                                     |
| `csv`              | `text/csv`                                                          | `attachment; filename="results.csv"`  |
| `xls`              | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | `attachment; filename="results.xlsx"` |

_**Response payload**_

Depends on `outputType`:

-   `json` (default): array of JSON objects, each one being a record result of the stored parameterized query.
-   `csv`: comma-separated values file. The first row contains column names. Values containing commas, double-quotes or
    newlines are quoted.
-   `xls`: Excel workbook (`.xlsx` format, Office Open XML). The first row contains column names.

_**Output type and NDJSON streaming**_

-   `outputType` applies to the standard (buffered) response path.
-   If the client sets the `Accept: application/x-ndjson` header, NDJSON streaming takes precedence over `outputType`
    and the server responds with `Content-Type: application/x-ndjson`, streaming one JSON object per line.
-   NDJSON output uses numeric types for integer columns (BigInt values are converted to numbers before serialization).
-   The `fresh` parameter can be combined with all output modes.
-   With `fresh=true` and `Accept: application/x-ndjson`, results are streamed incrementally from PostgreSQL using a
    cursor to avoid loading full result sets in memory.

_**Example Request (without DA parameters):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
[
    {
        "entityid": "alarm_nosignal_001",
        "entitytype": "template",
        "__ALERTDESCRIPTION__": "Regla que evalua si llegan medidas",
        "__NAME__": "nosignal_001",
        "__SEVERITY__": "medium",
        "__TIME_BETWEEN_NOTIF__": "3600000",
        "templateid": "alarm_nosenal_usuario",
        "__ATTR__": null,
        "__OPER__": null,
        "__UMBRAL__": null,
        "created_at": "2026-02-11 10:41:17.960528"
    },
    {
        "entityid": "alarm_threshold_04",
        "entitytype": "template",
        "__ALERTDESCRIPTION__": "Alerta de nivel de llenado por superación de umbral",
        "__NAME__": "threshold_04",
        "__SEVERITY__": "medium",
        "__TIME_BETWEEN_NOTIF__": "1800000",
        "templateid": "comparacion_umbral_usuario",
        "__ATTR__": "fillingLevel",
        "__OPER__": ">=",
        "__UMBRAL__": 0.9,
        "created_at": "2026-02-11 10:41:17.960528"
    }
]
```

_**Example Request (with parameters):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_filter_by_name/data?pattern=%25nosignal%25" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
[
    {
        "entityid": "alarm_nosignal_001",
        "__NAME__": "nosignal_001",
        "__SEVERITY__": "medium"
    }
]
```

_**Example Request (CSV output):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data?outputType=csv" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  --output results.csv
```

_**Example Request (Excel output):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data?outputType=xls" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  --output results.xlsx
```

_**Example Request (NDJSON streaming):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath" \
  -H "Accept: application/x-ndjson"
```

_**Example Request (fresh query on PostgreSQL):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_filter_by_name/data?pattern=%25nosignal%25&fresh=true" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /servicePath"
```

#### Query `POST /plugin/cda/api/doQuery` (Pentaho CDA legacy support)

This endpoint provides backward compatibility with legacy Pentaho CDA clients.

It acts as a wrapper over the internal FDA query execution engine, adapting CDA-style requests into FDA-compatible
executions and transforming the response into CDA-compatible format.

This endpoint does **not** execute queries directly. It delegates execution to the FDA query engine.

_**Request headers**_

| Header           | Optional | Description                                                              | Example            |
| ---------------- | -------- | ------------------------------------------------------------------------ | ------------------ |
| `Content-Type`   |          | Must be `application/x-www-form-urlencoded`                              | —                  |
| `Fiware-Service` | ✓        | Tenant/service name. If not present, it is derived from the `path` field | `my-bucket`        |
| `Accept`         | ✓        | Currently ignored (response is always JSON CDA format)                   | `application/json` |

---

_**Request body (form-urlencoded)**_

The request body must be sent as `application/x-www-form-urlencoded`.

| Field          | Optional | Description                                                                                             | Example                             |
| -------------- | -------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `path`         |          | Path used to resolve service. FDA identifier defaults to `dataAccessId` unless `cda` field is provided. | `/public/service/verticals/sql/da1` |
| `dataAccessId` |          | Identifier of the Data Access (DA) inside the FDA                                                       | `da1`                               |
| `cda`          | ✓        | Explicit FDA identifier. If not provided, `dataAccessId` is used as FDA identifier                      | `fda1`                              |
| `outputType`   | ✓        | Format of the returned results. **Default:** `json`. Allowed values: `json`, `csv`, `xls`.              | `csv`                               |
| `param*`       | ✓        | Query parameters prefixed with `param`                                                                  | `parammunicipality=NA`              |
| `pageSize`     | ✓        | Pagination size (must be handled explicitly by the DA)                                                  | `10`                                |
| `pageStart`    | ✓        | Pagination offset (must be handled explicitly by the DA)                                                | `0`                                 |

---

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

| `outputType` value | `Content-Type`                                                      | `Content-Disposition`                 |
| ------------------ | ------------------------------------------------------------------- | ------------------------------------- |
| `json` (default)   | `application/json`                                                  | —                                     |
| `csv`              | `text/csv`                                                          | `attachment; filename="results.csv"`  |
| `xls`              | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | `attachment; filename="results.xlsx"` |

_**Response payload**_

Depends on `outputType`:

-   `json` (default): CDA-compatible structure:

```json
{
    "metadata": [{ "colIndex": 0, "colName": "column1" }, ...],
    "resultset": [["value1", "value2", ...]],
    "queryInfo": {
        "pageStart": 0,
        "pageSize": 10,
        "totalRows": 120
    }
}
```

-   `csv`: comma-separated values file with column names in the first row.
-   `xls`: Excel workbook (`.xlsx` format, Office Open XML) with column names in the first row.

_**Example Request (JSON, default):**_

```bash
curl -i -X POST "http://localhost:8085/plugin/cda/api/doQuery" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "Fiware-Service: my-bucket" \
  -d "path=/public/my-bucket/verticals/sql/da1" \
  -d "dataAccessId=da1" \
  -d "pageSize=10"
```

_**Example Response (JSON):**_

```json
{
    "metadata": [{ "colIndex": 0, "colName": "column1" }],
    "resultset": [["value1", "value2"]],
    "queryInfo": {
        "pageStart": 0,
        "pageSize": 10,
        "totalRows": 2
    }
}
```

_**Example Request (CSV output):**_

```bash
curl -i -X POST "http://localhost:8085/plugin/cda/api/doQuery" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "Fiware-Service: my-bucket" \
  -d "path=/public/my-bucket/verticals/sql/da1" \
  -d "dataAccessId=da1" \
  -d "outputType=csv" \
  --output results.csv
```

_**Example Request (Excel output):**_

```bash
curl -i -X POST "http://localhost:8085/plugin/cda/api/doQuery" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "Fiware-Service: my-bucket" \
  -d "path=/public/my-bucket/verticals/sql/da1" \
  -d "dataAccessId=da1" \
  -d "outputType=xls" \
  --output results.xlsx
```

---

## 🧭 Navigation

-   [⬅️ Previous: Architecture](/doc/02_architecture.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Config And Operational Guide](/doc/04_config_operational_guide.md)
