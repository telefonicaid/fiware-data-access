# Overview

**FIWARE Data Access (FDA)** is an API for reusable analytical data access. Cached datasets are stored in object
storage; only-fresh resources can query their source directly. FDA also provides a compatibility endpoint for selected
legacy Pentaho CDA clients, while native FDA endpoints are the recommended interface for new integrations.

The main concepts in FDA are:

-   **FDA (FIWARE Data Access)**: a data resource backed either by a cached Parquet dataset in object storage or by a
    direct, only-fresh query to its source.
-   **DA (Data Access)**: a parameterized query over a cached FDA. The legacy CDA endpoint provides a compatibility
    adapter for existing clients.

FDA interacts with these systems:

-   PostgreSQL and MongoDB (supported source databases)
-   CSV/XLS/XLSX uploads (alternative input for cached FDAs)
-   MinIO (object storage)
-   MongoDB (metadata and background-job persistence)
-   DuckDB (embedded Parquet transformation and query engine)

---

## 🧭 Navigation

-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Installation](/doc/01_installation.md)
