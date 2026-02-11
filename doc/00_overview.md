# Overview

**FIWARE Data Access (FDA)** is a smart city API designed to efficiently store and query datasets in an object storage
server.  
It replaces Pentaho CDA extensions and provides a modern, reusable approach to analytical data access.

The main concepts in FDA are:

-   **FDA (FIWARE Data Access)**: a precomputed, materialized dataset stored in Parquet format in object storage.
-   **DA (Data Access) / CDA (Community Data Access)**: parameterized queries executed on top of an FDA, producing JSON
    results.

FDA interacts with several systems:

-   PostgreSQL (source database)
-   MinIO (object storage)
-   MongoDB (metadata persistence)
-   DuckDB (query execution on Parquet datasets)

---

## 🧭 Navigation

-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Installation](/doc/01_installation.md)
