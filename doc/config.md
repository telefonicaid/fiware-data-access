# Configuration

-   [Introduction](#introduction)
-   [Variables](#variables)
    -   [Environment](#environment)
    -   [PostgreSQL](#postgresql)
    -   [Object bucket-based storage system](#object-bucket-based-storage-system)
    -   [MongoDB](#mongodb)

## Introduction

`Fiware-data-access` supports the configuration via environment file. To make this we have to create a `.env` file and
assign values to the desired variables. To do so we can copy the `.env.example` file and modify the variables.

## Variables

The _environment variabless_ are ordered inside the following categories.

### Environment

Variables related to the environment of the application:

| Variable       | Optional | Type   | Description                                                                                                           |
| -------------- | -------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `FDA_NODE_ENV` | ✓        | string | Level of the node environment. Possible values are `development` and `production`. Value is `development` by default. |

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

| Variable              | Optional | Type   | Description                                                                                          |
| --------------------- | -------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `FDA_OBJSTG_USER`     |          | string | User to connect to the object bucket-based storage system.                                           |
| `FDA_OBJSTG_PASSWORD` |          | string | Password to connect to the object bucket-based storage system.                                       |
| `FDA_OBJSTG_PROTOCOL` |          | string | Protocol (http or https) to connect to the object bucket-based storage system. Default value `https` |
| `FDA_OBJSTG_ENDPOINT` |          | string | Endpoint (host and port) to connect to the object bucket-based storage system.                       |

### MongoDB

Variables related to MongoDB:

| Variable             | Optional | Type   | Description                                           |
| -------------------- | -------- | ------ | ----------------------------------------------------- |
| `FDA_MONGO_USER`     |          | string | User to connect to the `MongoDB`.                     |
| `FDA_MONGO_PASSWORD` |          | string | Password to connect to the `MongoDB`.                 |
| `FDA_MONGO_ENDPOINT` |          | string | Endpoint (host and port) to connect to the `MongoDB`. |
