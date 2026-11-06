# Overview

**FIWARE Data Access (FDA)** is an API for reusable analytical data access. Cached datasets are stored in object
storage; only-fresh resources can query their source directly. FDA also provides a compatibility endpoint for selected
legacy Pentaho CDA clients, while native FDA endpoints are the recommended interface for new integrations.

The main concepts in FDA are:

-   **FDA (FIWARE Data Access)**: a data resource backed either by a cached Parquet dataset in object storage or by a
    direct, only-fresh query to its source.
-   **DA (Data Access)**: a parameterized query over an FDA, and the way its data is read. DAs of cached FDAs run on the
    Parquet dataset; DAs of only-fresh FDAs run against the source. The legacy CDA endpoint provides a compatibility
    adapter for existing clients.

FDA interacts with these systems:

-   PostgreSQL and MongoDB (supported source databases)
-   CSV/XLS/XLSX uploads (alternative input for cached FDAs)
-   MinIO (object storage)
-   MongoDB (metadata and background-job persistence)
-   DuckDB (embedded Parquet transformation and query engine)

The service runs as two independent roles, an API and a background Fetcher, that can share a process or be deployed
separately. See [Architecture](/doc/02_architecture.md#architecture-overview).

---

## 🧭 Navigation

-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Installation](/doc/01_installation.md)
