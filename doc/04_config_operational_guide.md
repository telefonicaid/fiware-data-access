# Configuration and Operational Guide

This document describes the configuration options and operational setup for **FIWARE Data Access** (FDA).  
The application is configured primarily through environment variables.

---

## Introduction

`Fiware-data-access` supports configuration via a `.env` file, or in docker service in
[`docker-compose.yml`](../docker/docker-compose.yml).

To set it up:

-   Copy the provided `.env.example` file located in `src/`:

```bash
cp src/.env.example .env
```

and modify the variables in the `.env` file according to your environment.

-   Or modify environment variables in `fda` service inside [`docker-compose.yml`](../docker/docker-compose.yml).

The variables are grouped by category: **Environment**, **PostgreSQL**, and **MongoDB**.

---

## Environment Variables

### Environment

Variables related to the environment of the application:

| Variable          | Optional | Type   | Description                                                                                                           |
| ----------------- | -------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `FDA_NODE_ENV`    | ✓        | string | Level of the node environment. Possible values are `development` and `production`. Value is `development` by default. |
| `FDA_SERVER_PORT` | ✓        | number | Port used by FDA server. Value is `8080` by default.                                                                  |

#### Instance Roles

Variables that define which components of the application are executed by this instance:

| Variable             | Optional | Type    | Description                                                                                              |
| -------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `FDA_ROLE_APISERVER` | ✓        | boolean | If `true`, the instance runs the API server to handle HTTP requests. Default `true`.                     |
| `FDA_ROLE_FETCHER`   | ✓        | boolean | If `true`, the instance runs the fetcher responsible for regenerating and updating FDAs. Default `true`. |

> Note: By default, an instance runs both roles (API server and Fetcher). You can disable one to separate
> responsibilities.

### PostgreSQL

Variables related to `PostgreSQL` client:

| Variable          | Optional | Type   | Description                                                                                          |
| ----------------- | -------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `FDA_PG_USER`     |          | string | User to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                              |
| `FDA_PG_PASSWORD` |          | string | Password to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                          |
| `FDA_PG_HOST`     |          | string | Host to connect to `PostgreSQL` to fetch the data to create the `FDAs`.                              |
| `FDA_PG_PORT`     | ✓        | number | Port to connect to `PostgreSQL` to fetch the data to create the `FDAs`. Value by _default_ **5432**. |

### Object bucket-based storage system

Variabes related to the object bucket-based storage system:

| Variable                   | Optional | Type   | Description                                                                                          |
| -------------------------- | -------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `FDA_OBJSTG_USER`          |          | string | User to connect to the object bucket-based storage system.                                           |
| `FDA_OBJSTG_PASSWORD`      |          | string | Password to connect to the object bucket-based storage system.                                       |
| `FDA_OBJSTG_PROTOCOL`      | ✓        | string | Protocol (http or https) to connect to the object bucket-based storage system. Default value `https` |
| `FDA_OBJSTG_ENDPOINT`      |          | string | Endpoint (host and port) to connect to the object bucket-based storage system.                       |
| `FDA_OBJSTG_MAX_POOL_SIZE` | ✓        | number | Max Pool size for connections pool for object storage. Default is 10                                 |

### MongoDB

Variables related to MongoDB:

| Variable        | Optional | Type   | Description                                                                                                                                                                         |
| --------------- | -------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FDA_MONGO_URI` |          | string | Mongodb connection URI to connect to the `MongoDB`. More details in [MongoDb connection URI](https://www.mongodb.com/docs/drivers/node/current/connect/mongoclient/#connection-uri) |

### Logger

| Variable           | Optional | Type   | Description                                                                                      |
| ------------------ | -------- | ------ | ------------------------------------------------------------------------------------------------ |
| `FDA_LOG_LEVEL`    | ✓        | string | Value to define the log level. Possible values `INFO`, `WARN`, `ERROR`, `DEBUG`, default `INFO`. |
| `FDA_LOG_COMP`     | ✓        | string | Name of the component for the log context. Default value `FDA`                                   |
| `FDA_LOG_RES_SIZE` | ✓        | number | Size in characters of the response body showed in the logs. Default value `100`                  |

---

## Example `.env` file

```env
# NODE ENVIRONMENT
FDA_NODE_ENV=development

# POSTGRESQL
FDA_PG_USER=exampleUser
FDA_PG_PASSWORD=examplePass
FDA_PG_HOST=exampleHost
FDA_PG_PORT=5432

## Object Bucket-Based Storage System
FDA_OBJSTG_PROTOCOL=http
FDA_OBJSTG_ENDPOINT=endpoint:port
FDA_OBJSTG_USER=exampleUser
FDA_OBJSTG_PASSWORD=examplePass

# MONGODB
FDA_MONGO_URI=mongodb://exampleUser:examplePassword@endpoint:port

# Instance Roles
FDA_ROLE_APISERVER=true
FDA_ROLE_FETCHER=true
```

---

## Health Check Endpoints

FIWARE Data Access exposes one basic health check endpoint intended for monitoring, container orchestration systems
(e.g., Kubernetes), load balancers, or uptime checks.

This endpoint does **not** require authentication headers and can be used to verify that the service is running.

### `GET /health`

Basic root health check endpoint.

**Response code**

-   `200 OK` when the service is up.

**Response payload**

```json
{
    "status": "UP",
    "timestamp": "2026-02-16T10:15:30.123Z"
}
```

---

## 🧭 Navigation

-   [⬅️ Previous: Architecture](/doc/02_architecture.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Advanced Topics](/doc/05_advanced_topics.md)
