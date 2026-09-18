# FIWARE Data Access

[![CI](https://github.com/telefonicaid/fiware-data-access/workflows/CI/badge.svg)](https://github.com/telefonicaid/fiware-data-access/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/telefonicaid/fiware-data-access/badge.svg?branch=main)](https://coveralls.io/github/telefonicaid/fiware-data-access?branch=main)
[![Docker badge](https://img.shields.io/badge/docker-telefonicaiot%2Ffiware--data--access-blue?logo=docker)](https://hub.docker.com/r/telefonicaiot/fiware-data-access)

**FIWARE Data Access** is part of the FIWARE ecosystem and designed to expose analytical data through reusable and
parameterized APIs. It provides an efficient, open-source solution for storing and querying datasets in object storage,
replacing the legacy Pentaho CDA extension in smart city stacks.

> Every dataset tells a story. Some are just stored in Parquet.

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
-   [`03_api.md`](./doc/03_api.md) – API reference (also available as interactive
    [Swagger UI](https://swagger.io/tools/swagger-ui/) at `GET /api-docs` on a running instance)
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

---

##   License

Fiware-data-access is licensed under Affero General Public License (GPL) version 3.

© 2026 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
<details>
<summary><strong>Further information on the use of the AGPL open source license</strong></summary>
     
### Are there any legal issues with AGPL 3.0? Is it safe for me to use?

There is absolutely no problem in using a product licensed under AGPL 3.0. Issues with GPL
(or AGPL) licenses are mostly related with the fact that different people assign different
interpretations on the meaning of the term “derivate work” used in these licenses. Due to this,
some people believe that there is a risk in just _using_ software under GPL or AGPL licenses
(even without _modifying_ it).

For the avoidance of doubt, the owners of this software licensed under an AGPL-3.0 license
wish to make a clarifying public statement as follows:

> Please note that software derived as a result of modifying the source code of this
> software in order to fix a bug or incorporate enhancements is considered a derivative
> work of the product. Software that merely uses or aggregates (i.e. links to) an otherwise
> unmodified version of existing software is not considered a derivative work, and therefore
> it does not need to be released as under the same license, or even released as open source.

</details>
