# Fiware Data Access API Reference

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
