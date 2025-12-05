# Fiware Data Access API Reference

## Introduction

This document describes the API used by the FIWARE Data Access component.

This API is inspired in RESTful principles and we have two different resource types:

* **sets**: corresponding to a "raw" set, fetched from DB and corresponding to a Parquet file in MinIO
* **fdas**: corresponding to particular query over a set

There is a dependency relationship between the two types, as the *fdas* belongs to a given *set*.

The datamodel associated to this API (i.e. how sets and fdas are modeled in MongoDB) is out of the scope of this document.

## Error responses

TBD

## API Routes

### Set payload datamodel

A set is represented by a JSON object with the following fields:

| Parameter      | Optional | Type    | Description                 |
|----------------|----------|---------|-----------------------------|
| `id`           |          | string  | Set unique identifier                        |
| `description`  | ✓        | string  | A free text used by the client to describe the set  |
| `database`     |          | string  | Database from which the set has been created  |
| `table`        |          | string  | Table in the database from which the set has been created  |
| `bucket`       |          | string  | Bucket that stores the Parquet file storing the set in Minio  |
| `path`         |          | string  | Full path to the Parquet file storing the set in MinIO  |

### Sets operations

#### List Sets `GET /sets`

Returns a list of all the sets present in the system.

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 200 OK
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array containing one object per set. Each set follows the JSON set representation 
format (described in [Set payload datamodel](#set-payload-datamodel) section).

Example:

TBD

#### Create Set `POST /sets`

Creates a new set

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Content-Type`       |          | MIME type. Required to be `application/json`.         | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

The payload is a JSON object containing a set that follows the JSON set representation 
format (described in [Set payload datamodel](#set-payload-datamodel) section).

Example

TBD

_**Response code**_

* Successful operation uses 201 Created
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

* Return the header `Location` with the value of the path used to create the set (I.E : `/sets/st01`) 
when the creation succeeds (Response code 201).

_**Response payload**_

None

#### Get Set `GET /sets/{setId}`

Returns the set requested.

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 200 OK
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is a JSON object containing a set that follows the JSON set representation 
format (described in [Set payload datamodel](#set-payload-datamodel) section).

Example:

TBD

#### Regenerate Set `PUT /sets/{setId}`

Regenerate the set, fetching again the source table from DB.

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 204 No Content
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
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

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 204 No Content
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

None

_**Response payload**_

None

### FDAs operations

### FDA payload datamodel

A FDA is represented by a JSON object with the following fields:

| Parameter      | Optional | Type    | Description                 |
|----------------|----------|---------|-----------------------------|
| `id`           |          | string  | FDA identifier, unique within the associated set                       |
| `description`  | ✓        | string  | A free text used by the client to describe the FDA  |
| `query`        |          | string  | Query string to run over the set when invoking the FDA  |

#### List FDAs `GET /sets/{setId}/fdas`

Returns a list of all the FDAs associated to a given set.

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 200 OK
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is an array containing one object per FDA. Each FDA follows the JSON FDA representation 
format (described in [FDA payload datamodel](#fda-payload-datamodel) section).

Example:

TBD

#### Create FDA `POST /sets/{setId}/fdas`

Create a new FDA on a given set

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Content-Type`       |          | MIME type. Required to be `application/json`.         | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation 
format (described in [FDA payload datamodel](#fda-payload-datamodel) section).

Example:

TBD

_**Response code**_

* Successful operation uses 201 Created
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

* Return the header `Location` with the value of the path used to create the FDA (I.E : `/sets/st01/fdas/fda01`) 
when the creation succeeds (Response code 201).

_**Response payload**_

None

#### Get FDA `GET /sets/{setId}/fdas/{fdaId}`

Return the FDA requested

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 200 OK
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

Successful operations return `Content-Type` header with `application/json` value.

_**Response payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation 
format (described in [FDA payload datamodel](#fda-payload-datamodel) section).

Example:

TBD

#### Update FDA `PUT /sets/{setId}/fdas/{fdaId}`

Update FDA

_**Request query parameters**_

None so far

_**Request headers**_

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Content-Type`       |          | MIME type. Required to be `application/json`.         | `application/json` |
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

The payload is a JSON object containing a FDA that follows the JSON FDA representation 
format (described in [FDA payload datamodel](#fda-payload-datamodel) section). The FDA is updated with that content.

Example:

TBD

_**Response code**_

* Successful operation uses 204 No Content
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
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

| Header               | Optional | Description    | Example            |
|----------------------|----------|----------------|--------------------|
| `Fiware-Service`     |          | Tenant or service, using the common mechanism of the FIWARE platform      | `acme`    |

_**Request payload**_

None

_**Response code**_

* Successful operation uses 204 No Content
* Errors use a non-2xx and (optionally) an error payload. See subsection on [Error Responses](#error-responses) for
  more details.

_**Response headers**_

None

_**Response payload**_

None

### Non RESTful operations

#### doQuery

Same operation implemented by Pentaho CDA, in order to provide backward compatibility with existing CDA clients with minimal impact.

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

## Old draft API (to be removed)

## storeSet (TEMPORAL)

Stores a set in MinIO using parquet format.

⚠️ **Note:** In this stage of the initial development the sets are local files uploaded to MinIO. If the file is a CSV
the header must be separated by `,`. If the columns have the column data type next to the name the file is parsed and
transformed, generating and identical CSV without this data type annotations.

**Endpoint:** /storeSet

**Body:**

```json
{
    "fda": "newfda",
    "filePath": "lib/testSet.csv",
    "path": "s3://my-bucket/my-folder/"
}
```

| Key      | Type   | Description                      |
| :------- | :----- | :------------------------------- |
| fda      | string | Name of the set in MinIO         |
| filepath | string | Path of the local file to upload |
| path     | string | Path of the set in MinIO         |

## storeSetPG

Creates a set in Minio uploading a postgresql table.

**Endpoint:** /storeSetPG

**Body:**

```json
{
    "database": "pgDatabase",
    "table": "real_table",
    "bucket": "my-bucket",
    "path": "/performance/real_table"
}
```

| Key      | Type   | Description                                          |
| :------- | :----- | :--------------------------------------------------- |
| database | string | Database where the table is located                  |
| table    | string | Name of the table to upload to Minio                 |
| bucket   | string | Name of the bucket to store the set                  |
| path     | string | Path (folders and file name) of the new set in Minio |

## queryFDA

Queries a set.

**Endpoint:** /queryFDA

**Body:**

```json
{
    {
    "data": {
        "columns": "*",
        "filters": "id = 'id4'"
    },
    "cda": "newfda",
    "path": "s3://my-bucket/my-folder/"
}
}
```

| Key                  | Type   | Description              |
| :------------------- | :----- | :----------------------- |
| [data](#data-object) | object | Query properties         |
| cda                  | string | Name of the set in MinIO |
| path                 | string | Path of the set in MinIO |

### data object:

| Key     | Type   | Description                    |
| :------ | :----- | :----------------------------- |
| columns | string | Name of the columns to retrive |
| filters | string | Filter to apply to the query   |
