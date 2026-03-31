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

| Variable             | Description                                   | Example                 |
| -------------------- | --------------------------------------------- | ----------------------- |
| `url`                | API base endpoint including protocol and port | `http://localhost:8080` |
| `Fiware-Service`     | Header indicating the FIWARE service          | `my-service`            |
| `Fiware-ServicePath` | Header indicating the FIWARE service path     | `/servicePath`          |
| `visibility`         | FDA visibility segment in the URL path        | `public` or `private`   |
| `fdaId`              | Identifier of the FDA                         | `fda_alarms`            |
| `daId`               | Identifier of the DA                          | `da_all_alarms`         |

> ⚠️ These variables are required. The requests in the collection depend on them and will not work correctly if they are
> not properly configured.
