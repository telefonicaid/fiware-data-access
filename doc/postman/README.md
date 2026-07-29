# Postman Collection

Along with the project we include a `Postman` collection to use the API defined in the
[`api reference document`](../03_api.md).

## Using the Postman Collection

You can download Postman from:  
https://www.postman.com/downloads/

The Postman collection for this project is available at:  
https://github.com/telefonicaid/fiware-data-access/tree/main/doc/postman/fiware-data-access.postman_collection.json

---

### 1. Import the Collection

Import the `fiware-data-access.postman_collection.json` file into Postman.

---

### 2. Configure Environment Variables

Before sending requests, create or update a Postman environment and define the following variables:

| Variable             | Description                                        | Example                 |
| -------------------- | -------------------------------------------------- | ----------------------- |
| `url`                | API base endpoint including protocol and port      | `http://localhost:8080` |
| `Fiware-Service`     | Header indicating the FIWARE service               | `my-service`            |
| `Fiware-ServicePath` | Header indicating the FIWARE service path          | `/servicePath`          |
| `visibility`         | FDA visibility segment in the URL path             | `public` or `private`   |
| `datasourceId`       | Identifier of the Datasource                       | `pg_datasource`         |
| `fdaId`              | Identifier of the FDA                              | `fda_alarms`            |
| `daId`               | Identifier of the DA                               | `da_all_alarms`         |
| `auth_token`         | Optional token to include as `X-Auth-Token` header | `-`                     |

> ⚠️ These variables are required. The requests in the collection depend on them and will not work correctly if they are
> not properly configured.

---

### 3. Content negotiation for data endpoints

For `GET /{visibility}/fdas/{fdaId}/das/{daId}/data`, response format is selected only through the `Accept` header:

-   `application/json` (or missing/`*/*`) returns JSON.
-   `application/x-ndjson` returns NDJSON stream.
-   `text/csv` returns CSV stream.
-   `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` returns XLSX.

The same content negotiation rules apply to `GET /{visibility}/fdas/{fdaId}/data`, which is the direct fresh FDA query
endpoint.

---

### 4. Authentication

FIWARE-data-access does not implement any specific security mechanisms, it offers the `visibility` property so the user
can build his auth system on top. For those cases we added an optional `auth_token` variable to the requests thats gonna
be mapped to the `X-Auth-Token` header, in case the user has a system that requires token-based auth.

---

### 5. Uploading CSV/XLS/XLSX with Postman

To create FDAs from tabular files use `POST /{{visibility}}/fdas/upload` with **Body = form-data**:

-   Required fields:
    -   `id` (Text)
    -   `file` (File)
-   Optional fields:
    -   `description` (Text)
    -   `timeColumn` (Text)
    -   `objStgConf` (Text, JSON object string)
    -   `defaultDataAccess` (Text/Boolean)
    -   `datasourceId` (Text)

Example `objStgConf` value:

```json
{ "partition": "day", "compression": "zstd" }
```

Expected behavior:

-   `202` when upload is accepted and processing continues asynchronously.
-   `400` for invalid upload fields (`objStgConf.partition`, `objStgConf.compression`, malformed JSON, missing file).
-   `413` when file size exceeds the configured limit.
-   `415` when file type is not CSV/XLS/XLSX.
