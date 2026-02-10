# Fiware Data Access API Reference

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

If present, the error payload is a JSON object including the following fields:

-   `error`(required, string): a textual description of the error.
-   `description`(optional, string): additional information about the error.

FDA uses the HTTP status codes and error texts described in this section. However, the particular text used for
description field is thought for humans and its exact wording may vary between FDA versions.

The `error` reporting is as follows:

-   Errors which are only caused by request itself (i.e. they do not depend on the FDA status), either in the URL
    parameters or in the payload, results in `BadRequest`(`400`).
-   If the resource identified by the request is not found then status `404` is returned and depending the resource we
    get a `FDANotFound` or a `DaNotFound` code.
-   If the resource identified by the request is already in the database and we try to create it we get a
    `DuplicatedKey` (`409`).
-   Internal errors with status `500` use a different error code depending of the component that threw the error. `S3`
    component throws `S3ServerError`, `DuckDB` component throws `DuckDBServerError` and `MongoDB` throws
    `MongoDBServerError`.
-   Connection errors throw a status `503`, `PostgreSQL` component with a `UploadError` code and `MongoDB` component
    with a `MongoConnectionError` code.

## API Routes

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

Creates a new FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

Example

TBD

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the FDA (I.E : `/fdas/fda01`) when the
    creation succeeds (Response code 201).

_**Response payload**_

None

#### Get FDA `GET /fdas/{fdaId}`

Returns the FDA requested.

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

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section).

Example:

TBD

#### Regenerate FDA `PUT /fdas/{fdaId}`

Regenerate the FDA, fetching again the source table from DB.

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

Delete FDA. Note that deleting a FDA deletes in cascade all the DAs belonging to it.

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

| Header  | Optional | Description                                                          | Example |
| ------- | -------- | -------------------------------------------------------------------- | ------- |
| `fdaId` |          | Id of the `fda`. Must be unique in combination with `Fiware-Service` | `fda1`  |
| `daId`  |          | Id of the `da`. Must be unique inside each `fda`                     | `da1`   |

Additionally the necessary parameters for the query must be included with the previous ones.

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

The payload is an array of JSON objects, each one being a record result of the stored parameterized query.

```
[
    {
        "timeinstant": "2020-08-17T18:25:28.332Z",
        "activity": 12,
        "animalbreed": "Merina",
        "animalname": "TUNA",
        "animalspecies": "Ovino",
        ...
    },
    ...
]
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

The payload is an array of JSON objects, each one being a record result of the stored parameterized query.

```
[
    {
        "timeinstant": "2020-08-17T18:25:28.332Z",
        "activity": 12,
        "animalbreed": "Merina",
        "animalname": "TUNA",
        "animalspecies": "Ovino",
        ...
    },
    ...
]
```
