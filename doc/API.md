# Fiware Data Access API Reference

## fetchSet

Uploads a table from `postgresql` to `Minio`.

**Endpoint:** /fetchSet \
**Method** POST

**Body:**

```json
{
    "database": "pgDatabase",
    "table": "real_table",
    "bucket": "my-bucket",
    "path": "/performance/real_table.parquet"
}
```

| Key      | Type   | Description                                                         |
| :------- | :----- | :------------------------------------------------------------------ |
| database | string | Database where the table is located                                 |
| table    | string | Name of the table to upload to Minio                                |
| bucket   | string | Name of the bucket to store the set                                 |
| path     | string | Path (folders and file name with extension) of the new set in Minio |

## storeSet

Stores a set of queries in `mongodb`.

**Endpoint:** /storeSet \
**Method** POST

**Body:**

```json
{
    "bucket": "my-bucket",
    "path": "/performance/real_table",
    "query": "SELECT * FROM..."
}
```

| Key    | Type   | Description                                                                                                 |
| :----- | :----- | :---------------------------------------------------------------------------------------------------------- |
| bucket | string | Name of the bucket to store the set                                                                         |
| path   | string | Path (folders and file name) of the new set in Minio                                                        |
| query  | string | Parameterized query. The `FROM` clausule must reference the `Minio` document with the complete `Minio` url. |

## querySet

Runs a stored parameterized query. The value of the parameters must be included as url parameters.

**Endpoint:** /querySet \
**Method** GET

**URL Parameters:**

-   path: `Minio` path of the file to query
