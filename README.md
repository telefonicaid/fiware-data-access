# FIWARE Data Access

[![CI](https://github.com/telefonicaid/fiware-data-access/workflows/CI/badge.svg)](https://github.com/telefonicaid/fiware-data-access/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/telefonicaid/fiware-data-access/badge.svg?branch=main)](https://coveralls.io/github/telefonicaid/fiware-data-access?branch=main)
[![Docker badge](https://img.shields.io/badge/docker-telefonicaiot%2Ffiware--data--access-blue?logo=docker)](https://hub.docker.com/r/telefonicaiot/fiware-data-access)

**FIWARE Data Access** is a smart city API to efficiently store and query sets in an object storage server. It's
designed as a open source replacement for Pentaho CDAs extension.

Initial development

---

## Run:

docker compose -f docker/docker-compose.yml up mc

⚠️ For the creation of the table to be effective the minIO bucket must be created by hand using the web GUI or the minio
client.

---

## 📦 Main Components

-   🔄 **Fiware Data Access**  
     Node.js service that provides an API to store and query sets in minio.

-   🔌 **DuckDB**  
     High-performance analytical database system. With the `httpfs` extension DuckDB supports reading/writing/globbing files
    on object storage servers using the S3 API.

-   🏢 **MinIO object storage**  
     Object storage system compatible with S3 and capable of working with unstructured data. Stores the sets in parquet format.

---

## 🚀 Purpose

-   Replace Pentaho CDAs in FIWARE smart city stacks.

---

## 📁 Documentation

Complete documentation is available in the [`doc/`](./doc/) directory:

-   [`API Reference`](./doc/API.md): - API endpoint documentation
-   [`Main concepts`](./doc/concepts.md): - FDA Concepts
-   [`Performance measuring`](./doc/performance.md): - Documentation for the file with performance measuring utilities.
-   [`Database Model`](./doc/database_model.md): - Documentation describing the structure of the MongoDB.
-   [`Advanced Topics`](./doc/advanced.md): - Advanced topics about the application.
-   [`Environment Configuration`](./doc/config.md): - Information about the environment variables and configuration.
-   [`Testing strategy`](./doc/Testing.md): - Information about the testing strategy.

---

## 🛠️ Requirements
