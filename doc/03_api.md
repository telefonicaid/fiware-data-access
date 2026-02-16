# Fiware Data Access API Reference

## Table of Contents

-   [Introduction](#introduction)
-   [Error Responses](#error-responses)
-   [API Routes](#api-routes)
    -   [FDA payload datamodel](#fda-payload-datamodel)
    -   [FDAs operations](#fdas-operations)
        -   [List FDAs](#list-fdas-get-fdas)
        -   [Create FDA](#create-fda-post-fdas)
        -   [Get FDA](#get-fda-get-fdasfdaid)
        -   [Regenerate FDA](#regenerate-fda-put-fdasfdaid)
        -   [Delete FDA](#delete-fda-delete-fdasfdaid)
    -   [DA payload datamodel](#da-payload-datamodel)
    -   [DAs operations](#das-operations)
        -   [List DAs](#list-das-get-fdasfdaiddas)
        -   [Create DA](#create-da-post-fdasfdaiddas)
        -   [Get DA](#get-da-get-fdasfdaiddasdaid)
        -   [Update DA](#update-da-put-fdasfdaiddasdaid)
        -   [Delete DA](#delete-da-delete-fdasfdaiddasdaid)
-   [Non RESTful operations](#non-restful-operations)
    -   [Query](#query-get-query)
    -   [Query (Pentaho CDA legacy support)](#query-get-doquery-pentaho-cda-legacy-support)
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

| Code | Status                | Error Code             | Cause                                                                                                                                                                                     |
| ---- | --------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 400  | Bad Request           | `BadRequest`           | Missing or invalid values in request body, headers, or query parameters. Request errors that do not depend on the FDA status. The `Fiware-Service` header is required for all operations. |
| 404  | Not Found             | `FDANotFound`          | The requested FDA was not found.                                                                                                                                                          |
| 404  | Not Found             | `DaNotFound`           | The requested Data Access (DA) was not found.                                                                                                                                             |
| 409  | Conflict              | `DuplicatedKey`        | The resource already exists in the database. Attempting to create a duplicate resource.                                                                                                   |
| 500  | Internal Server Error | `S3ServerError`        | An error occurred in the S3 object storage component.                                                                                                                                     |
| 500  | Internal Server Error | `DuckDBServerError`    | An error occurred in the DuckDB component.                                                                                                                                                |
| 500  | Internal Server Error | `MongoDBServerError`   | An error occurred in the MongoDB component.                                                                                                                                               |
| 503  | Service Unavailable   | `UploadError`          | Connection error with the PostgreSQL database component.                                                                                                                                  |
| 503  | Service Unavailable   | `MongoConnectionError` | Connection error with the MongoDB component.                                                                                                                                              |

### Common error scenarios

#### Missing required header

When the `Fiware-Service` header is not provided:

**Request:**

```bash
curl -i http://localhost:8080/fdas
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
curl -i http://localhost:8080/query \
  -H "Fiware-Service: my-bucket"
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
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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

#### Resource not found (FDA)

When requesting an FDA that doesn't exist:

**Request:**

```bash
curl -i http://localhost:8080/fdas/nonexistent \
  -H "Fiware-Service: my-bucket"
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
curl -i http://localhost:8080/fdas/fda_alarms/das/nonexistent-da \
  -H "Fiware-Service: my-bucket"
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
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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
    "description": "An FDA with id 'fda_alarms' already exists"
}
```

#### Database connection error

When a connection error occurs with a backend service:

**Request:**

```bash
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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

## Health Endpoint

This endpoint allow checking whether the FIWARE Data Access service is running.

It does not require the `Fiware-Service` header and is intended for monitoring purposes.

---

### Health Check `GET /health`

Returns the operational status of the service.

**Request headers**

None required.

**Response code**

-   `200 OK` — Service is running.

**Response payload**

```json
{
    "status": "UP",
    "timestamp": "2026-02-16T10:15:30.123Z"
}
```

### FDA payload datamodel

A FDA is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                                                   |
| ------------- | -------- | ------ | ----------------------------------------------------------------------------- |
| `id`          |          | string | FDA unique identifier                                                         |
| `description` | ✓        | string | A free text used by the client to describe the FDA                            |
| `query`       |          | string | Base `postgreSQL` query to create the file in the bucket-based storage system |

### FDAs operations

#### List FDAs `GET /fdas`

Returns a list of all the FDAs present in the system.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/fdas \
  -H "Fiware-Service: my-bucket"
```

_**Example Response:**_

```json
[
    {
        "_id": "698c572d1cd0982695cc3a8e",
        "fdaId": "fda_alarms",
        "query": "SELECT * FROM public.alarms",
        "das": {},
        "service": "my-bucket",
        "servicePath": "/public",
        "description": "FDA de alarmas del sistema"
    }
]
```

#### Create FDA `POST /fdas`

Creates a new FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description                                                                                                                                                    | Example            |
| -------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `Content-Type`       |          | MIME type. Required to be `application/json`.                                                                                                                  | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform                                                                                           | `my-bucket`        |
| `Fiware-ServicePath` | ✓        | Hierarchical service path to allow a `FDA` to be queried with authentication or anonimaly. Possible values `/public` and `/private`. Default value `/private`. | `/public`          |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /public" \
  -d '{
    "id": "fda_alarms",
    "query": "SELECT * FROM public.alarms",
    "description": "FDA de alarmas del sistema"
  }'
```

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the FDA (I.E : `/fdas/fda01`) when the
    creation succeeds (Response code 201).

_**Response payload**_

None

_**Example Response:**_

```
HTTP/1.1 201 Created
X-Powered-By: Express
Content-Type: text/plain; charset=utf-8
Content-Length: 7

Created
```

#### Get FDA `GET /fdas/{fdaId}`

Returns the FDA requested.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

None

_**Example Request:**_

```bash
curl -i -X GET http://localhost:8080/fdas/fda_alarms \
  -H "Fiware-Service: my-bucket"
```

_**Example Response:**_

```
{
    "_id": "698c572d1cd0982695cc3a8e",
    "fdaId": "fda_alarms",
    "query": "SELECT * FROM public.alarms",
    "das": {},
    "service": "my-bucket",
    "servicePath": "/public",
    "description": "FDA de alarmas del sistema"
}
```

#### Regenerate FDA `PUT /fdas/{fdaId}`

Regenerate the FDA, fetching again the source table from DB.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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

#### Delete FDA `DELETE /fdas/{fdaId}`

Delete FDA. Note that deleting a FDA deletes in cascade all the DAs belonging to it.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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

### DAs operations

### DA payload datamodel

A DA is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                           |
| ------------- | -------- | ------ | ----------------------------------------------------- |
| `id`          |          | string | DA identifier, unique within the associated FDA       |
| `description` | ✓        | string | A free text used by the client to describe the DA     |
| `query`       |          | string | Query string to run over the FDA when invoking the DA |

#### List DAs `GET /fdas/{fdaId}/das`

Returns a list of all the DAs associated to a given FDA.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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
curl -i -X GET http://localhost:8080/fdas/fda_alarms/das \
  -H "Fiware-Service: my-bucket"
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

#### Create DA `POST /fdas/{fdaId}/das`

Create a new DA on a given FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`        |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

_**Example Request:**_

```bash
curl -i -X POST http://localhost:8080/fdas/fda_alarms/das \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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

-   Return the header `Location` with the value of the path used to create the DA (I.E : `/fdas/fda01/das/da01`) when
    the creation succeeds (Response code 201).

_**Response payload**_

None

#### Get DA `GET /fdas/{fdaId}/das/{daId}`

Return the DA requested

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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
curl -i -X GET http://localhost:8080/fdas/fda_alarms/das/da_all_alarms \
  -H "Fiware-Service: my-bucket"
```

_**Example Response:**_

```json
{
  "description": "Todas las alarmas",
  "query": "SELECT * LIMIT 10",
  "id": "da_all_alarms"
},
{
  "das": {},
  "service": "my-bucket",
  "description": "FDA de alarmas del sistema"
}
```

#### Update DA `PUT /fdas/{fdaId}/das/{daId}`

Update DA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket`        |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section). The DA is updated with that content.

_**Example Request:**_

```bash
curl -i -X PUT http://localhost:8080/fdas/fda_alarms/das/da_all_alarms \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
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

#### Delete DA `DELETE /fdas/{fdaId}/das/{daId}`

Delete DA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

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

### Non RESTful operations

#### Query `GET /query`

Runs a stored parameterized query. The value of the parameters must be included as url parameters.

_**Request query parameters**_

| Header  | Optional | Description                                                          | Example |
| ------- | -------- | -------------------------------------------------------------------- | ------- |
| `fdaId` |          | Id of the `fda`. Must be unique in combination with `Fiware-Service` | `fda1`  |
| `daId`  |          | Id of the `da`. Must be unique inside each `fda`                     | `da1`   |

Additionally the necessary parameters for the query must be included with the previous ones.

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array of JSON objects, each one being a record result of the stored parameterized query.

_**Content negotiation (JSON / NDJSON)**_

-   By default the endpoint returns a full JSON array (`application/json`). This keeps backward compatibility with
    existing clients.
-   If the client sets the `Accept: application/x-ndjson` header the server responds with
    `Content-Type: application/x-ndjson` and streams one JSON object per line (NDJSON). Use this for large result sets
    or streaming consumers.
-   NDJSON output uses numeric types for integer columns (BigInt values are converted to numbers before serialization).

_**Example Request (without parameters):**_

```bash
curl -i -X GET "http://localhost:8080/query?fdaId=fda_alarms&daId=da_all_alarms" \
  -H "Fiware-Service: my-bucket"
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
curl -i -X GET "http://localhost:8080/query?fdaId=fda_alarms&daId=da_filter_by_name&pattern=%nosignal%" \
  -H "Fiware-Service: my-bucket"
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

_**Example Request (NDJSON):**_

```bash
curl -i -X GET "http://localhost:8080/query?fdaId=fda_alarms&daId=da_all_alarms" \
  -H "Fiware-Service: my-bucket" \
  -H "Accept: application/x-ndjson"
```

#### Query `GET /doQuery` (Pentaho CDA legacy support)

Same operation implemented by Pentaho CDA, in order to provide backward compatibility with existing CDA clients with
minimal impact. This method is a kind of wrapper of `query`

_**Request query parameters**_

| Header         | Optional | Description                                                                 | Example        |
| -------------- | -------- | --------------------------------------------------------------------------- | -------------- |
| `path`         |          | Path to the `fda`. Right now only uses the last bit to retrieve the `fdaId` | `/public/fda1` |
| `dataAccessId` |          | Id of the `da`. Must be unique inside each `fda`                            | `da1`          |

Additionally the necessary parameters for the query must be included with the previous ones.

_**Request headers**_

| Header           | Optional | Description                                                          | Example     |
| ---------------- | -------- | -------------------------------------------------------------------- | ----------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `my-bucket` |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array of JSON objects, each one being a record result of the stored parameterized query.

_**Content negotiation (JSON / NDJSON)**_

-   By default the endpoint returns a full JSON array (`application/json`). This is the backward-compatible behaviour.
-   If the client sets the `Accept: application/x-ndjson` header the server responds with
    `Content-Type: application/x-ndjson` and streams one JSON object per line (NDJSON). This is useful for large results
    and streaming processing.
-   NDJSON output will have numeric types for integer columns (BigInt values are converted to numbers before
    serialization).

_**Example Request:**_

```bash
curl -i -X GET "http://localhost:8080/doQuery?path=/public/fda_alarms&dataAccessId=da_all_alarms" \
  -H "Fiware-Service: my-bucket"
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

_**Example Request (NDJSON):**_

```bash
curl -i -X GET "http://localhost:8080/doQuery?path=/public/fda_alarms&dataAccessId=da_all_alarms" \
  -H "Fiware-Service: my-bucket" \
  -H "Accept: application/x-ndjson"
```

---

## 🧭 Navigation

-   [⬅️ Previous: Architecture](/doc/02_architecture.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Config And Operational Guide](/doc/04_config_operational_guide.md)
