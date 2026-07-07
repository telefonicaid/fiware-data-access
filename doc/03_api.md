# Fiware Data Access API Reference

## Table of Contents

-   [Introduction](#introduction)
-   [Error Responses](#error-responses)
-   [API Routes](#api-routes)
    -   [Health Endpoint](#health-endpoint)
        -   [Health Check `GET /health`](#health-check-get-health)
    -   [Metrics Endpoint](#metrics-endpoint)
        -   [Retrieve metrics `GET /metrics`](#retrieve-metrics-get-metrics)
    -   [Datasource payload datamodel](#datasource-payload-datamodel)
    -   [Datasources operations](#datasources-operations)
        -   [List Datasources](#list-datasources-get-datasources)
        -   [Create Datasource](#create-datasource-post-datasources)
        -   [Get Datasource](#get-datasource-get-datasourcesdatasourceid)
        -   [Update Datasource](#update-datasource-put-datasourcesdatasourceid)
        -   [Delete Datasource](#delete-datasource-delete-datasourcesdatasourceid)
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
        -   [FDA data query](#fda-data-query-get-visibilityfdasfdaiddata)
        -   [Data Access query](#data-access-query-get-visibilityfdasfdaiddasdaiddata)
        -   [Query (Pentaho CDA legacy support)](#query-plugincdaapidoquery-pentaho-cda-legacy-support)
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

| Code | Status                | Error Code                  | Cause                                                                                                                                                                                                                                                                                                              |
| ---- | --------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 400  | Bad Request           | `BadRequest`                | Missing or invalid values in request body, headers, or query parameters. `Fiware-Service`, `Fiware-ServicePath`, and `visibility` (path segment) are required for all operations.                                                                                                                                  |
| 400  | Bad Request           | `BadRequest`                | Invalid or unsupported query fields were provided (for example, using `fresh` in `GET /{visibility}/fdas/{fdaId}/das/{daId}/data` or any query string in `GET /{visibility}/fdas/{fdaId}/data`).                                                                                                                   |
| 400  | Bad Request           | `InvalidVisibility`         | The `visibility` path segment is not one of the allowed values (`public`, `private`).                                                                                                                                                                                                                              |
| 400  | Bad Request           | `InvalidServicePath`        | The `Fiware-ServicePath` header value is not a valid non-root absolute path (e.g. `/servicePath/site`). The root path `/` is not allowed.                                                                                                                                                                          |
| 400  | Bad Request           | `InvalidQueryParam`         | Some of the params in the request don't comply with the [params](#params) array restrictions.                                                                                                                                                                                                                      |
| 400  | Bad Request           | `PartitionError`            | Some of the params related to the creation of the parquet partition don't comply with the [object storage configuration](#object-storage-configuration-objstgconf) requirements.                                                                                                                                   |
| 400  | Bad Request           | `CleaningError`             | Trying to remove a non partitioned FDA or incorrect value in the [delete interval key](#refresh-policy-object).                                                                                                                                                                                                    |
| 403  | Forbidden             | `VisibilityMismatch`        | The FDA exists but was created under a different `visibility`. Cannot access a private FDA through a public route and vice-versa.                                                                                                                                                                                  |
| 404  | Not Found             | `FDANotFound`               | The requested FDA was not found.                                                                                                                                                                                                                                                                                   |
| 404  | Not Found             | `DaNotFound`                | The requested Data Access (DA) was not found.                                                                                                                                                                                                                                                                      |
| 404  | Not Found             | `DatasourceNotFound`        | The requested datasource does not exist for the provided `Fiware-Service` (for example during FDA creation, or when resolving datasource credentials for existing FDA operations). See [Operational note about DatasourceNotFound](/doc/04_config_operational_guide.md#operational-note-about-datasourcenotfound). |
| 406  | Not Acceptable        | `NotAcceptable`             | `Accept` header does not allow any supported response format (`application/json`, `application/x-ndjson`, `text/csv`, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`).                                                                                                                        |
| 409  | Conflict              | `DuplicatedKey`             | The resource already exists in the database. Attempting to create a duplicate resource.                                                                                                                                                                                                                            |
| 409  | Conflict              | `FDAUnavailable`            | FDA `exampleId` is not queryable yet because the first fetch has not completed.                                                                                                                                                                                                                                    |
| 409  | Conflict              | `FDANotOnlyFresh`           | The FDA is cached (`cached=true`) and cannot be queried through `GET /{visibility}/fdas/{fdaId}/data`; use a DA instead.                                                                                                                                                                                           |
| 409  | Conflict              | `FDAOnlyFresh`              | The FDA was created with `cached=false`, so it does not allow DAs nor cached DA queries.                                                                                                                                                                                                                           |
| 409  | Conflict              | `RequestStyleConflict`      | The request mixes query-style context (`service`, `servicePath` in query params) with legacy `Fiware-Service`/`Fiware-ServicePath` headers in the same data-query URL. Use only one style per request.                                                                                                             |
| 409  | Conflict              | `DatasourceInUse`           | Attempted to delete a datasource that is currently referenced by one or more FDAs in the same `Fiware-Service`.                                                                                                                                                                                                    |
| 429  | Too Many Requests     | `TooManyFreshQueries`       | The number of concurrent direct fresh FDA queries exceeded `FDA_MAX_CONCURRENT_FRESH_QUERIES`.                                                                                                                                                                                                                     |
| 500  | Internal Server Error | `S3ServerError`             | An error occurred in the S3 object storage component.                                                                                                                                                                                                                                                              |
| 500  | Internal Server Error | `DuckDBServerError`         | An error occurred in the DuckDB component.                                                                                                                                                                                                                                                                         |
| 500  | Internal Server Error | `MongoDBServerError`        | An error occurred in the MongoDB component.                                                                                                                                                                                                                                                                        |
| 400  | Bad Request           | `UnsupportedDatasourceType` | The referenced datasource type is not supported by the operation (supported types are `postgres` and `mongodb`).                                                                                                                                                                                                   |
| 400  | Bad Request           | `InvalidMongoFDAContract`   | Invalid Mongo-specific FDA payload. Mongo FDAs require a Mongo query definition inside `query`, support only cached mode (`cached=true`), and do not support `refreshPolicy.type=window`.                                                                                                                          |
| 503  | Service Unavailable   | `UploadError`               | Connection error with the PostgreSQL database component.                                                                                                                                                                                                                                                           |
| 503  | Service Unavailable   | `SyncQueriesDisabled`       | A direct FDA query was sent but the API instance is running with `FDA_ROLE_SYNCQUERIES=false`.                                                                                                                                                                                                                     |
| 503  | Service Unavailable   | `MongoConnectionError`      | Connection error with the MongoDB component.                                                                                                                                                                                                                                                                       |

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
  -H "Fiware-Service: trantor" \
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
  -H "Fiware-Service: trantor" \
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
  -H "Fiware-Service: trantor" \
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
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath"
```

**Response (404):**

```json
{
    "error": "FDANotFound",
    "description": "FDA nonexistent not found in service trantor"
}
```

#### Resource not found (DA)

When requesting a Data Access that doesn't exist:

**Request:**

```bash
curl -i http://localhost:8080/public/fdas/fda_alarms/das/nonexistent-da \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath"
```

**Response (404):**

```json
{
    "error": "DaNotFound",
    "description": "DA nonexistent-da not found in FDA fda_alarms and service trantor."
}
```

#### Duplicate resource

When attempting to create a resource that already exists:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: trantor" \
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
    {"description":"FDA with id fda_alarms and trantor already exists: MongoServerError: E11000 duplicate key error collection: fiware-data-access.fdas index: fdaId_1_service_1 dup key: { fdaId: \"fda_alarms\", service: \"trantor\" }"}
}
```

#### Database connection error

When a connection error occurs with a backend service:

**Request:**

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: trantor" \
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
  -H "Fiware-Service: trantor" \
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
fda_tenant_requests_total{fiware_service="trantor",fiware_service_path="/",method="GET",route="/:visibility/fdas",status_class="2xx"} 8
# HELP fda_catalog_fdas_by_service Number of FDA documents by fiware service and servicePath.
# TYPE fda_catalog_fdas_by_service gauge
fda_catalog_fdas_by_service{fiware_service="trantor",fiware_service_path="/"} 12
# HELP fda_jobs_agenda_total Total number of Agenda jobs stored in MongoDB.
# TYPE fda_jobs_agenda_total gauge
fda_jobs_agenda_total 7
...
# EOF
```

### Datasource payload datamodel

A datasource is represented by a JSON object with the following fields:

| Parameter      | Optional | Type   | Description                                                                                                                            |
| -------------- | -------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `datasourceId` |          | string | Datasource identifier, unique within a given `Fiware-Service`.                                                                         |
| `type`         |          | string | Datasource type. Currently supported: `postgres`, `mongodb`.                                                                           |
| `config`       |          | object | Datasource connection configuration. For `postgres`: `user`, `password`, `host`, `port`, `database`. For `mongodb`: `uri`, `database`. |

### Datasources operations

Datasource operations are scoped by `Fiware-Service` header.

#### List Datasources `GET /datasources`

Returns all datasources for the provided `Fiware-Service`.

_**Request path parameters**_

None.

_**Request query parameters**_

None.

_**Request headers**_

| Header           | Optional | Description                                                          | Example   |
| ---------------- | -------- | -------------------------------------------------------------------- | --------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor` |

_**Request payload**_

None.

_**Response code**_

-   Successful operation uses `200 OK`.
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses).

_**Response headers**_

Successful operations return `Content-Type: application/json`.

_**Response payload**_

Array of datasource objects.

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/datasources \
  -H "Fiware-Service: trantor"
```

_**Example Response:**_

```json
[
    {
        "datasourceId": "default",
        "type": "postgres",
        "config": {
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "trantor"
        }
    }
]
```

#### Create Datasource `POST /datasources`

Creates a datasource for the provided `Fiware-Service`.

_**Request path parameters**_

None.

_**Request query parameters**_

None.

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`          |

_**Request payload**_

JSON object following [Datasource payload datamodel](#datasource-payload-datamodel).

Required body fields: `datasourceId`, `type`, `config`.

Datasource creation validates the connection before persisting the datasource.

Validation is datasource-specific:

-   `postgres`: validates by opening a PostgreSQL connection.
-   `mongodb`: validates by opening a MongoDB connection.

_**Response code**_

-   Successful operation uses `200 OK`.
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses).

_**Response headers**_

None.

_**Response payload**_

None.

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/datasources \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -d '{
        "datasourceId": "default",
        "type": "postgres",
        "config": {
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "trantor"
        }
    }'
```

_**Example Request (MongoDB datasource):**_

```bash
curl -i -X POST http://localhost:8080/datasources \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -d '{
        "datasourceId": "mongo-default",
        "type": "mongodb",
        "config": {
            "uri": "mongodb://localhost:27017",
            "database": "trantor"
        }
    }'
```

#### Get Datasource `GET /datasources/{datasourceId}`

Returns one datasource from the provided `Fiware-Service`.

_**Request path parameters**_

| Parameter      | Optional | Description                               | Example   |
| -------------- | -------- | ----------------------------------------- | --------- |
| `datasourceId` |          | Datasource identifier within the service. | `default` |

_**Request query parameters**_

None.

_**Request headers**_

| Header           | Optional | Description                                                          | Example   |
| ---------------- | -------- | -------------------------------------------------------------------- | --------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor` |

_**Request payload**_

None.

_**Response code**_

-   Successful operation uses `200 OK`.
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses).

_**Response headers**_

Successful operations return `Content-Type: application/json`.

_**Response payload**_

Datasource object.

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/datasources/default \
    -H "Fiware-Service: trantor"
```

#### Update Datasource `PUT /datasources/{datasourceId}`

Updates one datasource from the provided `Fiware-Service`.

_**Request path parameters**_

| Parameter      | Optional | Description                               | Example   |
| -------------- | -------- | ----------------------------------------- | --------- |
| `datasourceId` |          | Datasource identifier within the service. | `default` |

_**Request query parameters**_

None.

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`          |

_**Request payload**_

Allowed body fields: `type`, `config`.

When present, the resulting datasource configuration is validated before the update is stored.

_**Response code**_

-   Successful operation uses `204 No Content`.
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses).

_**Response headers**_

None.

_**Response payload**_

None.

_**Example Request:**_

```bash
curl -i -X PUT http://localhost:8080/datasources/default \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -d '{
        "type": "postgres",
        "config": {
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "trantor"
        }
    }'
```

#### Delete Datasource `DELETE /datasources/{datasourceId}`

Deletes one datasource from the provided `Fiware-Service`.

Deletion is rejected with `409 Conflict` while any FDA in that service is still using the datasource. For datasource
`default`, legacy FDAs without an explicit `datasourceId` are also considered users of that datasource.

_**Request path parameters**_

| Parameter      | Optional | Description                               | Example   |
| -------------- | -------- | ----------------------------------------- | --------- |
| `datasourceId` |          | Datasource identifier within the service. | `default` |

_**Request query parameters**_

None.

_**Request headers**_

| Header           | Optional | Description                                                          | Example   |
| ---------------- | -------- | -------------------------------------------------------------------- | --------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor` |

_**Request payload**_

None.

_**Response code**_

-   Successful operation uses `204 No Content`.
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses).

_**Response headers**_

None.

_**Response payload**_

None.

_**Example Request:**_

```bash
curl -i -X DELETE http://localhost:8080/datasources/default \
    -H "Fiware-Service: trantor"
```

The delete operation does not validate whether existing FDAs reference the datasource. Any dependent FDA operation will
fail later if datasource resolution is required.

### FDA payload datamodel

A FDA is represented by a JSON object with the following fields:

| Parameter                                                | Optional | Type          | Description                                                                                                                                                                                |
| -------------------------------------------------------- | -------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`                                                     |          | string        | FDA unique identifier                                                                                                                                                                      |
| `description`                                            | ✓        | string        | A free text used by the client to describe the FDA. If omitted, no description is stored.                                                                                                  |
| `query`                                                  |          | string/object | Source query definition. For `postgres`, a SQL query string. For `mongodb`, a Mongo query definition detailed below.                                                                       |
| `refreshPolicy`                                          | ✓        | object        | Optional policy for automatic refresh.                                                                                                                                                     |
| [`objStgConf`](#object-storage-configuration-objstgconf) | ✓        | object        | Various options to configure the FDA uploaded in the object storage app.                                                                                                                   |
| `timeColumn`                                             | ✓        | string        | Required with `refreshPolicy` of type `window` and `partition`. Column in the table indicating when the data was received (date).                                                          |
| `cached`                                                 | ✓        | boolean       | If `false`, the FDA is created as only-fresh: no parquet snapshot is maintained, no DAs are allowed, and the FDA is queried through `GET /{visibility}/fdas/{fdaId}/data`. Default `true`. |
| `datasourceId`                                           | ✓        | string        | Datasource id used to resolve DB credentials for this FDA. If omitted, FDA uses `default`.                                                                                                 |
| `skipBootstrap`                                          | ✓        | boolean       | If `true`, skips synchronous bootstrap during creation (one-row parquet + default DA creation). First refresh job is still scheduled. Default `false`.                                     |

For MongoDB datasources, `query` is an object with the following keys:

| Field        | Type   | Description                                                                |
| ------------ | ------ | -------------------------------------------------------------------------- |
| `collection` | string | MongoDB collection name.                                                   |
| `filter`     | object | MongoDB filter document.                                                   |
| `projection` | object | MongoDB projection document defining the fields materialized into the FDA. |

In MongoDB, projection can include more complex operators like `$slice` or `$elemMatch`. See the
[MongoDB projection documentation](https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/) for
details. Nested MongoDB fields can be projected using dot notation:

```json
{
    "projection": {
        "device.name": 1,
        "status": 1
    }
}
```

Projected nested fields are materialized as FDA columns preserving their dot notation (`device.name`).

When generating [defaultDataAccess](AdvancedTopics/default_data_access.md) parameters, dots are replaced by underscores.
For example:

-   Column: `device.name`
-   Parameter: `device_name`

Datasource-specific constraints:

-   Mongo datasource FDAs are currently cached-only (`cached=true`).
-   Mongo datasource FDAs do not support `refreshPolicy.type=window`.
-   If `timeColumn` is provided for Mongo FDAs, it must be included in `query.projection` (or the `query.projection`
    omitted at all as in this case no projection is done and all fields are retrieved).

#### Refresh Policy object

Defines how and when the FDA should be automatically refreshed.

| Field               | Optional | Type   | Description                                             |
| ------------------- | -------- | ------ | ------------------------------------------------------- |
| `type`              |          | string | Refresh strategy. One of: `none`, `interval`, `window`. |
| [`params`](#params) | (\*)     | object | Object with the parameters for the refresh policy type. |

(\*) Not used when `type` is `none`, mandatory otherwise

If omitted, the default policy is:

```json
{ "type": "none" }
```

##### Params

| Field             | Optional | Type   | Description                                                                                                                                                                                                                                                                                                                                                       |
| ----------------- | -------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `refreshInterval` |          | string | It represents a human interval (e.g. `1 hour`) or a cron expression. The frequency for the scheduled refresh and clean jobs. Must be minor or equal to partition size (if existing) (value of the field [`objstgconf.partition`](#object-storage-configuration-objstgconf)).                                                                                      |
| `fetchSize`       |          | string | **Only for type `window`**, it can take the values `hour`, `day`, `week`, `month` and `year`. Represents the time range of data to fetch (e.g. last hour/month data). If [`objStgConf.partition`](#object-storage-configuration-objstgconf) is set, it must be equal to that partition size; in practice `hour` is only valid when no partitioning is configured. |
| `windowSize`      | ✓        | string | Temporal interval of data we are gonna keep in storage (e.g. only the data of the last month). Possible values: `day`, `week`, `month` and `year`. If omitted, then all data is kept forever, no clean partition is done (i.e. an "infinite" window).                                                                                                             |

##### Object storage configuration (objstgconf)

This object configures certain aspects of the object storage app when uploading an FDA. The possible keys are:

| Parameter     | Optional | Type    | Description                                                                                                                                           |
| ------------- | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `partition`   | ✓        | string  | Tells how the FDA data should be partitioned in the object storage app. Possible values: `day`, `week`, `month` and `year`. Default: no partitioning. |
| `compression` | ✓        | boolean | Tells if the FDA parquet file should be compressed (using `ZSTD` compression) or not. Default: `false` (no compression).                              |

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

Returns a list of all the FDAs for that `service`, `servicePath` and `visibility`.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
[
    {
        "id": "fda_alarms",
        "datasourceId": "default",
        "query": "SELECT * FROM public.alarms",
        "das": {},
        "status": "completed",
        "progress": 100,
        "lastFetch": "2026-02-19T07:38:21.263Z",
        "refreshPolicy": {
            "type": "interval",
            "params": { "refreshInterval": "1 hour" }
        },
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

| Parameter           | Optional | Description                                                                                                                                                                                                                      | Example |
| ------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `defaultDataAccess` | ✓        | Overrides the instance default and enables or disables automatic `defaultDataAccess` creation for this FDA. The default value is taken from `FDA_CREATE_DEFAULT_DATA_ACCESS`; if that env var is not set, the default is `true`. | `false` |

When `skipBootstrap=true` is sent in the request body, it has priority during creation and initial `defaultDataAccess`
generation is skipped even if this query parameter is enabled.

_**Request headers**_

| Header               | Optional | Description                                                          | Example            |
| -------------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`       |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`          |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Stored and exact-matched on access.  | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

`skipBootstrap=true` is useful for heavy queries where synchronous bootstrap could delay creation. In this mode FDA
creation returns normally and first fetch is delegated to the background job.

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "fda_alarms",
        "datasourceId": "default",
    "query": "SELECT * FROM public.alarms",
    "description": "FDA de alarmas del sistema",
    "refreshPolicy": {
        "type": "interval",
        "params": { "refreshInterval": "1 hour" }
    },
    "cached": true
  }'
```

_**Example Request disabling default DA:**_

```bash
curl -i -X POST "http://localhost:8080/public/fdas?defaultDataAccess=false" \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath" \
    -d '{
        "id": "fda_alarms_no_default",
        "query": "SELECT * FROM public.alarms",
        "description": "FDA without default DA"
    }'
```

_**Example Request for an only-fresh FDA:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath" \
    -d '{
        "id": "fda_live_alarms",
        "query": "SELECT * FROM public.alarms",
        "description": "Only-fresh FDA",
        "cached": false
    }'
```

_**Example Request with bootstrap skip:**_

```bash
curl -i -X POST "http://localhost:8080/public/fdas?defaultDataAccess=true" \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath" \
    -d '{
        "id": "fda_heavy_query",
        "query": "SELECT * FROM public.very_large_table",
        "description": "Create without synchronous bootstrap",
        "skipBootstrap": true,
        "cached": true
    }'
```

_**Example Request for a cached Mongo FDA:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas \
    -H "Content-Type: application/json" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath" \
    -d '{
        "id": "fda_mongo_events",
        "datasourceId": "mongo-default",
        "query": {
            "collection": "events",
            "filter": {
            "site": "lab"
            },
            "projection": {
            "device": 1,
            "status": 1,
            "reading": 1
            }
        },
        "description": "Mongo cached FDA",
        "cached": true
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

```json
HTTP/1.1 202 Accepted
X-Powered-By: Express
Content-Type: application/json; charset=utf-8

{
    "id": "fda_alarms",
    "status": "pending"
}
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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Response:**_

```json
{
    "datasourceId": "default",
    "query": "SELECT * FROM public.alarms",
    "das": {},
    "status": "completed",
    "progress": 100,
    "lastFetch": "2026-02-19T07:38:21.263Z",
    "refreshPolicy": {
        "type": "interval",
        "params": { "refreshInterval": "1 hour" }
    },
    "description": "FDA de alarmas del sistema"
}
```

#### Regenerate FDA `PUT /{visibility}/fdas/{fdaId}`

Regenerate the FDA, fetching again the source table from DB.

The operation may return `409 Conflict` in the following cases:

-   If the FDA is currently being processed.
-   If the FDA is configured as _fresh-only_ (non-cached) and does not support manual regeneration.

_**Request path parameters**_

| Parameter    | Optional | Description                                                | Example  |
| ------------ | -------- | ---------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private` | `public` |
| `fdaId`      |          | Id of the FDA                                              | `fda1`   |

_**Request query parameters**_

None so far

_**Request payload**_

None.

_**Request headers**_

| Header               | Optional | Description                                                          | Example        |
| -------------------- | -------- | -------------------------------------------------------------------- | -------------- |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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

| Parameter     | Optional | Type   | Description                                                                                            |
| ------------- | -------- | ------ | ------------------------------------------------------------------------------------------------------ |
| `id`          | (\*)     | string | DA identifier, unique within the associated FDA.                                                       |
| `description` | ✓        | string | A free text used by the client to describe the DA. If omitted, no description is stored.               |
| `query`       |          | string | Query string, without **FROM**, clause to run over the FDA when invoking the DA                        |
| `params`      | ✓        | array  | Array of [param objects](#params) to control param values. If omitted, the DA has no query parameters. |

(\*) The `id` field is mandatory when creating a DA (`POST`) and must not be included when updating a DA (`PUT`).

#### Params

Each object in the array `params` can have the following keys:

| Parameter  | Optional | Type    | Description                                                                                                                                                                                                                                          |
| ---------- | -------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`     |          | string  | Name of the param to control.                                                                                                                                                                                                                        |
| `type`     |          | string  | Type of the param to enforce. Possible values: _Number_, _Boolean_, _Text_ and _DateTime_.                                                                                                                                                           |
| `required` | ✓        | boolean | Tell if the param must be provided by the user. Default value _false_. If `true` and the value is missing, request fails even when `default` is defined.                                                                                             |
| `default`  | ✓        | string  | Provide a default value for the param in case it isn't provided. It is applied only when the value is missing and `required` is `false`. If omitted, no default is applied.                                                                          |
| `range`    | ✓        | array   | Array with the minimun and maximun value (`Number`) a param can take. The array should be consistent (only two elements and the first one lesser than the second), otherwise an error will be responsed. If omitted, no range validation is applied. |
| `enum`     | ✓        | array   | Array with all the possible values (`Number` or `Text`) a param can take. If omitted, no enum validation is applied.                                                                                                                                 |

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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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
  -H "Fiware-Service: trantor" \
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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`          |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/public/fdas/fda_alarms/das \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath" \
  -d '{
    "id": "da_all_alarms",
    "description": "Todas las alarmas",
    "query": "SELECT * LIMIT 10"
  }'
```

_**Example Response:**_

```
HTTP/1.1 200 OK
X-Powered-By: Express
Content-Type: text/plain; charset=utf-8
Content-Length: 7

Created
```

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

None.

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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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
  -H "Fiware-Service: trantor" \
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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`          |
| `Fiware-ServicePath` |          | NGSI hierarchical service path. Must match the FDA's stored path.    | `/servicePath`     |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section). The DA is updated with that content.

_**Example Request:**_

```bash
curl -i -X PUT http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: trantor" \
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
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform | `trantor`      |
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

#### FDA data query `GET /{visibility}/fdas/{fdaId}/data`

Runs the FDA base query directly against PostgreSQL. This endpoint is always fresh and does not use the parquet cache.

_**Request path parameters**_

| Parameter    | Optional | Description                                                          | Example  |
| ------------ | -------- | -------------------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private`           | `public` |
| `fdaId`      |          | Id of the `fda`. Must be unique in combination with `Fiware-Service` | `fda1`   |

_**Request query parameters**_

The endpoint supports two request styles:

| Parameter     | Optional | Description                                                                                             | Example        |
| ------------- | -------- | ------------------------------------------------------------------------------------------------------- | -------------- |
| `service`     | ✓        | Tenant or service. Required when using query-style context (instead of FIWARE headers).                 | `trantor`      |
| `servicePath` | ✓        | NGSI hierarchical service path. Required when using query-style context (instead of FIWARE headers).    | `/servicePath` |
| `outputType`  | ✓        | Output format for query-style context. Allowed values: `json`, `ndjson`, `csv`, `xls`. Default: `json`. | `csv`          |

When using header-style context, any query string parameter is rejected with `400 BadRequest`.

_**Request headers**_

| Header               | Optional | Description                                              | Example        |
| -------------------- | -------- | -------------------------------------------------------- | -------------- |
| `Fiware-Service`     | ✓        | Tenant or service for header-style context.              | `trantor`      |
| `Fiware-ServicePath` | ✓        | NGSI hierarchical service path for header-style context. | `/servicePath` |

`Fiware-Service` and `Fiware-ServicePath` cannot be mixed with query-style context (`service`, `servicePath` query
params). If both styles are present, API returns `409 RequestStyleConflict`.

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Content negotiation and serialization notes**_

-   In header-style context, response format is negotiated through the `Accept` header (using the
    [standard HTTP content negotiation mechanism](https://datatracker.ietf.org/doc/html/rfc2616#section-12)).
-   In query-style context, response format is controlled by `outputType` query parameter.
-   This endpoint requires `FDA_ROLE_SYNCQUERIES=true` in the API instance.
-   In query-style context, no additional query parameters are allowed besides `service`, `servicePath`, and
    `outputType`; if the client attempts to send any other query parameter, the API returns `400 BadRequest` with
    `FDA fresh query does not accept query parameters`.
-   With `Accept: application/x-ndjson` and `Accept: text/csv`, results are streamed incrementally from PostgreSQL using
    a cursor.

_**Example Request:**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_live_alarms/data" \
    -H "Fiware-Service: trantor" \
    -H "Fiware-ServicePath: /servicePath"
```

_**Example Request (query-style context):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_live_alarms/data?service=trantor&servicePath=%2FservicePath"
```

#### Data Access query `GET /{visibility}/fdas/{fdaId}/das/{daId}/data`

Runs a stored parameterized query for the selected DA. The request path declares the access visibility and the query
string carries DA parameters.

_**Request path parameters**_

| Parameter    | Optional | Description                                                          | Example  |
| ------------ | -------- | -------------------------------------------------------------------- | -------- |
| `visibility` |          | FDA access visibility. Allowed values: `public`, `private`           | `public` |
| `fdaId`      |          | Id of the `fda`. Must be unique in combination with `Fiware-Service` | `fda1`   |
| `daId`       |          | Id of the `da`. Must be unique inside each `fda`                     | `da1`    |

_**Request query parameters**_

The endpoint supports two request styles:

| Parameter     | Optional | Description                                                                                                    | Example                  |
| ------------- | -------- | -------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `service`     | ✓        | Tenant or service. Required when using query-style context (instead of FIWARE headers).                        | `trantor`                |
| `servicePath` | ✓        | NGSI hierarchical service path. Required when using query-style context (instead of FIWARE headers).           | `/servicePath`           |
| `outputType`  | ✓        | Output format for query-style context. Allowed values: `json`, `ndjson`, `csv`, `xls`, `cda`. Default: `json`. | `csv`                    |
| DA params     | ✓        | DA-specific parameters declared in `params`.                                                                   | `pattern=%25nosignal%25` |

_**Request headers**_

| Header               | Optional | Description                                              | Example        |
| -------------------- | -------- | -------------------------------------------------------- | -------------- |
| `Fiware-Service`     | ✓        | Tenant or service for header-style context.              | `trantor`      |
| `Fiware-ServicePath` | ✓        | NGSI hierarchical service path for header-style context. | `/servicePath` |

`Fiware-Service` and `Fiware-ServicePath` cannot be mixed with query-style context (`service`, `servicePath` query
params). If both styles are present, API returns `409 RequestStyleConflict`.

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

| `Accept` request header                                                                           | `Content-Type`                                                      | `Content-Disposition`                 |
| ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------- |
| `application/json`, missing, or `*/*`                                                             | `application/json`                                                  | —                                     |
| `application/x-ndjson`                                                                            | `application/x-ndjson`                                              | —                                     |
| `text/csv`                                                                                        | `text/csv; charset=utf-8`                                           | `attachment; filename="results.csv"`  |
| `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` or `application/vnd.ms-excel` | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | `attachment; filename="results.xlsx"` |

_**Response payload**_

Depends on `Accept`:

-   `application/json` (or default): array of JSON objects, each one being a record result of the stored parameterized
    query.
-   `application/x-ndjson`: one JSON object per line (streamed response).
-   `text/csv`: comma-separated values file. The first row contains column names. Values containing commas,
    double-quotes or newlines are quoted.
-   spreadsheet MIME types: Excel workbook (`.xlsx` format, Office Open XML). The first row contains column names.
-   `application/vnd.fiware.cda+json`: CDA-compatible JSON structure:

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

_**Content negotiation and serialization notes**_

-   In header-style context, response format is negotiated through the `Accept` header (using the
    [standard HTTP content negotiation mechanism](https://datatracker.ietf.org/doc/html/rfc2616#section-12)).
-   In query-style context, response format is controlled by `outputType` query parameter.
-   The CDA-compatible JSON representation can be requested using `outputType=cda` in query-style context or
    `Accept: application/vnd.fiware.cda+json` in header-style context. It returns a tabular JSON payload (`metadata`,
    `resultset`, `queryInfo`).
-   If `Accept` does not include a supported format in header-style context, the API returns `406 NotAcceptable`.
-   Unsupported query fields are rejected with `400 BadRequest`.
-   `fresh` query field is rejected with `400 BadRequest`.
-   Date values are normalized to strings (ISO 8601) before JSON/NDJSON/CSV serialization.
-   Integer database values are normalized to numeric JSON values.
-   With `Accept: text/csv` and `Accept: application/x-ndjson`, responses are streamed.

_**Example Request (without DA parameters):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: trantor" \
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
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath"
```

_**Example Request (query-style context):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_filter_by_name/data?service=trantor&servicePath=%2FservicePath&pattern=%25nosignal%25"
```

_**Example Request (query-style CDA-compatible JSON):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_filter_by_name/data?service=trantor&servicePath=%2FservicePath&outputType=cda&pattern=%25nosignal%25"
```

_**Example Request (CSV output):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath" \
    -H "Accept: text/csv" \
  --output results.csv
```

_**Example Request (Excel output):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath" \
    -H "Accept: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" \
  --output results.xlsx
```

_**Example Request (NDJSON streaming):**_

```bash
curl -i -X GET "http://localhost:8080/public/fdas/fda_alarms/das/da_all_alarms/data" \
  -H "Fiware-Service: trantor" \
  -H "Fiware-ServicePath: /servicePath" \
  -H "Accept: application/x-ndjson"
```

#### Query `/plugin/cda/api/doQuery` (Pentaho CDA legacy support)

This endpoint provides backward compatibility with legacy Pentaho CDA clients.

It acts as a wrapper over the internal FDA query execution engine, adapting CDA-style requests into FDA-compatible
executions and transforming the response into CDA-compatible format.

This endpoint does **not** execute queries directly. It delegates execution to the FDA query engine.

Supported methods:

-   `GET /plugin/cda/api/doQuery`
-   `POST /plugin/cda/api/doQuery`

_**Request headers**_

| Header           | Optional | Description                                                              | Example            |
| ---------------- | -------- | ------------------------------------------------------------------------ | ------------------ |
| `Content-Type`   | ✓        | For `POST`, should be `application/x-www-form-urlencoded`                | —                  |
| `Fiware-Service` | ✓        | Tenant/service name. If not present, it is derived from the `path` field | `trantor`          |
| `Accept`         | ✓        | Ignored when `outputType` is provided. If omitted, defaults to JSON      | `application/json` |

---

_**Request payload**_

For `POST`, send as `application/x-www-form-urlencoded` body.

For `GET`, send as query parameters.

| Field          | Optional | Description                                                                                                                                                                                                                             | Example                              |
| -------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `path`         |          | Path used to resolve context (`visibility`, `service`, and FDA id). Supported formats include `/public/<service>/...` and `home/<service>/verticals/public/<fda>.cda`. If no explicit FDA id is present, it defaults to `dataAccessId`. | `/public/service/verticals/sql/fda1` |
| `dataAccessId` |          | Identifier of the Data Access (DA) inside the FDA                                                                                                                                                                                       | `da1`                                |
| `outputType`   | ✓        | Format of the returned results. **Default:** `json`. Allowed values: `json`, `csv`, `xls`.                                                                                                                                              | `csv`                                |
| `param*`       | ✓        | Query parameters prefixed with `param`. If omitted, no DA query parameters are passed.                                                                                                                                                  | `parammunicipality=NA`               |
| `pageSize`     | ✓        | Pagination size (must be handled explicitly by the DA). If omitted, this field is not passed and DA defaults apply.                                                                                                                     | `10`                                 |
| `pageStart`    | ✓        | Pagination offset (must be handled explicitly by the DA). If omitted, this field is not passed and DA defaults apply.                                                                                                                   | `0`                                  |

---

_**Path-to-scope convention (legacy CDA)**_

In this compatibility endpoint, `visibility` and `servicePath` are resolved from `path` to preserve Pentaho legacy
behavior:

-   `visibility`: extracted from `path` (`public` or `private`)
-   `servicePath`: normalized as `/${visibility}`

This convention is intentional and backward compatible with legacy clients that did not manage `Fiware-ServicePath`.

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
  -H "Fiware-Service: trantor" \
  -d "path=/public/trantor/verticals/sql/da1" \
  -d "dataAccessId=da1" \
  -d "pageSize=10"
```

_**Example Request (GET + query params, JSON default):**_

```bash
curl -i -X GET "http://localhost:8085/plugin/cda/api/doQuery?path=/public/trantor/verticals/sql/fda1&dataAccessId=da1&paramminAge=25&pageSize=10&pageStart=0"
```

_**Example Request (GET legacy CKAN/Pentaho style + CSV):**_

```bash
curl -i -X GET "http://localhost:8085/plugin/cda/api/doQuery?path=home/trantor/verticals/public/environment.cda&dataAccessId=airqualityobserved&paramstart=2023-01-01%2000%3A00%3A00&paramfinish=2023-03-30%2023%3A59%3A59&_TRUST_USER_=opendata_trantor&outputType=csv" \
    --output results.csv
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
  -H "Fiware-Service: trantor" \
  -d "path=/public/trantor/verticals/sql/fda1" \
  -d "dataAccessId=da1" \
  -d "outputType=csv" \
  --output results.csv
```

_**Example Request (Excel output):**_

```bash
curl -i -X POST "http://localhost:8085/plugin/cda/api/doQuery" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "Fiware-Service: trantor" \
  -d "path=/public/trantor/verticals/sql/fda1" \
  -d "dataAccessId=da1" \
  -d "outputType=xls" \
  --output results.xlsx
```

---

## 🧭 Navigation

-   [⬅️ Previous: Architecture](/doc/02_architecture.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Config And Operational Guide](/doc/04_config_operational_guide.md)
