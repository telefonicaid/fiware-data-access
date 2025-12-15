# Fiware Data Access API Reference

## Introduction

This document describes the API used by the FIWARE Data Access component.

This API is inspired in RESTful principles and we have two different resource types:

-   **fdas**: corresponding to a "raw" fda, fetched from DB and corresponding to a Parquet file in MinIO
-   **data accesses (das)**: corresponding to particular query over a fda

There is a dependency relationship between the two types, as the _das_ belongs to a given _fda_.

The datamodel associated to this API (i.e. how fdas and das are modeled in MongoDB) is out of the scope of this
document.

## Error responses

TBD

## API Routes

### FDA payload datamodel

A fda is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                                  |
| ------------- | -------- | ------ | ------------------------------------------------------------ |
| `id`          |          | string | FDA unique identifier                                        |
| `description` | ✓        | string | A free text used by the client to describe the FDA           |
| `database`    |          | string | Database from which the FDA has been created                 |
| `table`       |          | string | Table in the database from which the FDA has been created    |
| `bucket`      |          | string | Bucket that stores the Parquet file storing the FDA in Minio |
| `path`        |          | string | Full path to the Parquet file storing the FDA in MinIO       |

### FDAs operations

#### List FDAs `GET /fdas`

Returns a list of all the FDAs present in the system.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

Example:

TBD

#### Create FDA `POST /fdas`

Creates a new fda

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a fda that follows the JSON fda representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

Example

TBD

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the fda (I.E : `/fdas/fda01`) when the
    creation succeeds (Response code 201).

_**Response payload**_

None

#### Get FDA `GET /fdas/{fdaId}`

Returns the fda requested.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 OK
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is a JSON object containing a fda that follows the JSON fda representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

Example:

TBD

#### Regenerate FDA `PUT /fdas/{fdaId}`

Regenerate the fda, fetching again the source table from DB.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

Delete fda. Note that deleting a fda deletes in cascade all the das belonging to it.

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

Example:

TBD

#### Create DA `POST /fdas/{fdaId}/das`

Create a new DA on a given FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section).

Example:

TBD

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

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

Example:

TBD

#### Update DA `PUT /fdas/{fdaId}/das/{daId}`

Update DA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a DA that follows the JSON DA representation format (described in
[DA payload datamodel](#da-payload-datamodel) section). The DA is updated with that content.

Example:

TBD

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

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

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

TBD (formerly we mention `path` here, but now sure if we are using it at the end...)

_**Request headers**_

| Header           | Optional | Description                                                          | Example |
| ---------------- | -------- | -------------------------------------------------------------------- | ------- |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`  |

_**Request payload**_

None

_**Response code**_

-   Successful operation uses 200 No Content
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

TBD

#### doQuery (Petaho CDA legacy support)

Same operation implemented by Pentaho CDA, in order to provide backward compatibility with existing CDA clients with
minimal impact. This method is a kind of wrapper of `queryFDA`

_**Request query parameters**_

TBD

_**Request headers**_

TBD

_**Request payload**_

TBD

_**Response code**_

TBD

_**Response headers**_

TBD

_**Response payload**_

TBD
