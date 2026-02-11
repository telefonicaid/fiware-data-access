# Fiware Data Access

[![CI](https://github.com/telefonicaid/fiware-data-access/workflows/CI/badge.svg)](https://github.com/telefonicaid/fiware-data-access/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/telefonicaid/fiware-data-access/badge.svg?branch=main)](https://coveralls.io/github/telefonicaid/fiware-data-access?branch=main)
[![Docker badge](https://img.shields.io/badge/docker-telefonicaiot%2Ffiware--data--access-blue?logo=docker)](https://hub.docker.com/r/telefonicaiot/fiware-data-access)

**FIWARE Data Access** is part of the FIWARE ecosystem and designed to expose analytical data through reusable and
parameterized APIs. It provides an efficient, open-source solution for storing and querying datasets in object storage,
replacing the legacy Pentaho CDA extension in smart city stacks.

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
-   Provide an efficient, modern data access layer for analytical queries.

---

## 📁 Documentation

Complete documentation is available in the [`doc/`](./doc/) directory:

-   [`00_overview.md`](./doc/00_overview.md) – Project overview
-   [`01_installation.md`](./doc/01_installation.md) – How to install & run (includes Docker section)
-   [`02_architecture.md`](./doc/02_architecture.md) – System architecture, main concepts and database model
-   [`03_api.md`](./doc/03_api.md) – API reference
-   [`04_config_operational_guide.md`](./doc/04_config_operational_guide.md) – Configuration and operational guide
-   [`05_advanced_topics.md`](./doc/05_advanced_topics.md) – Advanced topics
-   [`06_testing.md`](./doc/06_testing.md) – Test strategy and execution
-   [`07_performance.md`](./doc/07_performance.md) – Performance measurement utilities

---

## 🛠️ Requirements

-   Node.js >= 24
-   Docker & docker-compose
-   PostgreSQL
-   MongoDB
-   MinIO
