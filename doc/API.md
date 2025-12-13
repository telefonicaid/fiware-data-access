# Fiware Data Access API Reference

## Introduction

This document describes the API used by the FIWARE Data Access component.

This API is inspired in RESTful principles and we have two different resource types:

-   **sets**: corresponding to a "raw" set, fetched from DB and corresponding to a Parquet file in MinIO
-   **fdas**: corresponding to particular query over a set

There is a dependency relationship between the two types, as the _fdas_ belongs to a given _set_.

The datamodel associated to this API (i.e. how sets and fdas are modeled in MongoDB) is out of the scope of this
document.

## Error responses

TBD

## API Routes

### Set payload datamodel

A set is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                                  |
| ------------- | -------- | ------ | ------------------------------------------------------------ |
| `id`          |          | string | Set unique identifier                                        |
| `description` | ✓        | string | A free text used by the client to describe the set           |
| `database`    |          | string | Database from which the set has been created                 |
| `table`       |          | string | Table in the database from which the set has been created    |
| `bucket`      |          | string | Bucket that stores the Parquet file storing the set in Minio |
| `path`        |          | string | Full path to the Parquet file storing the set in MinIO       |

### Sets operations

#### List Sets `GET /fdas`

Returns a list of all the fdas present in the system.

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

The payload is an array containing one object per set. Each set follows the JSON set representation format (described in
[Set payload datamodel](#set-payload-datamodel) section).

Example:

TBD

#### Create Set `POST /sets`

Creates a new set

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a set that follows the JSON set representation format (described in
[Set payload datamodel](#set-payload-datamodel) section).

Example

TBD

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the set (I.E : `/sets/st01`) when the
    creation succeeds (Response code 201).

_**Response payload**_

None

#### Get Set `GET /sets/{setId}`

Returns the set requested.

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

The payload is a JSON object containing a set that follows the JSON set representation format (described in
[Set payload datamodel](#set-payload-datamodel) section).

Example:

TBD

#### Regenerate Set `PUT /sets/{setId}`

Regenerate the set, fetching again the source table from DB.

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

#### Delete Set `DELETE /sets/{setId}`

Delete set. Note that deleting a set deletes in cascade all the FDAs belonging to it.

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

### FDAs operations

### FDA payload datamodel

A FDA is represented by a JSON object with the following fields:

| Parameter     | Optional | Type   | Description                                            |
| ------------- | -------- | ------ | ------------------------------------------------------ |
| `id`          |          | string | FDA identifier, unique within the associated set       |
| `description` | ✓        | string | A free text used by the client to describe the FDA     |
| `query`       |          | string | Query string to run over the set when invoking the FDA |

#### List FDAs `GET /sets/{setId}/fdas`

Returns a list of all the FDAs associated to a given set.

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

#### Create FDA `POST /sets/{setId}/fdas`

Create a new FDA on a given set

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

Example:

TBD

_**Response code**_

-   Successful operation uses 201 Created
-   Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
    more details.

_**Response headers**_

-   Return the header `Location` with the value of the path used to create the FDA (I.E : `/sets/st01/fdas/fda01`) when
    the creation succeeds (Response code 201).

_**Response payload**_

None

#### Get FDA `GET /sets/{setId}/fdas/{fdaId}`

Return the FDA requested

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

#### Update FDA `PUT /sets/{setId}/fdas/{fdaId}`

Update FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header           | Optional | Description                                                          | Example            |
| ---------------- | -------- | -------------------------------------------------------------------- | ------------------ |
| `Content-Type`   |          | MIME type. Required to be `application/json`.                        | `application/json` |
| `Fiware-Service` |          | Tenant or service, using the common mechanism of the FIWARE platform | `acme`             |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation format (described in
[FDA payload datamodel](#fda-payload-datamodel) section). The FDA is updated with that content.

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

#### Delete FDA `DELETE /sets/{setId}/fdas/{fdaId}`

Delete FDA

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

#### Query `GET /querySet`

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
minimal impact. This method is a kind of wrapper of `querySet`

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
