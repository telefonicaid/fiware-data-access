# Testing

## Test Strategy

This project uses a **mixed testing strategy**:

-   unit tests for isolated query-building, validation and error-path logic
-   end-to-end integration tests for real cross-system behavior

The goal is to validate both:

-   deterministic internal logic close to the source of change
-   the full data flow across external dependencies as it runs in production

---

### Approach

-   Tests are written using **Jest**
-   Unit tests use targeted mocks for isolated modules and edge cases
-   Integration tests exercise the API using **real HTTP requests**

Integration tests start **real service instances** using Docker.

---

### Testcontainers

All external dependencies are started dynamically using **Testcontainers**:

-   **PostgreSQL / PostGIS**
-   **MongoDB**
-   **MinIO (S3-compatible object storage)**

Each test run:

-   Starts isolated containers
-   Uses random host ports
-   Cleans up automatically after execution

This guarantees:

-   No shared state between test runs
-   No dependency on local services
-   Reproducible results in CI and locally

---

### Application Execution Model

The FDA application is started as a **separate Node.js child process** during tests.

This mirrors real deployment behavior and avoids:

-   Shared module state with the test runner
-   False positives caused by in-process mocks

The test flow is:

1. Start containers (Postgres, MongoDB, MinIO)
2. Seed PostgreSQL with test data
3. Launch the FDA application as a child process
4. Wait until the API is reachable
5. Execute HTTP requests against the running service
6. Validate responses
7. Shut down the application and containers

---

### What Is Tested

Unit tests validate, among others:

-   parameter validation and coercion
-   DA/FDA query composition
-   error propagation and cleanup behavior
-   API route wiring and request validation

Integration tests validate:

-   FDA creation (PostgreSQL → CSV → Parquet → MinIO)
-   Metadata persistence in MongoDB
-   DuckDB query execution over Parquet files
-   End-to-end API behavior using real data
-   direct FDA fresh execution over PostgreSQL
-   default DA behavior including automatic creation and optional filters

---

### Coverage Considerations

Because the application runs as a **separate process** in integration mode, standard Jest coverage:

-   Does not instrument the application process
-   Under-reports integration-executed application code unless extra instrumentation is used

Coverage can be collected using additional tooling (e.g. `c8`). In practice, meaningful coverage in this repository
comes from combining:

-   unit test execution inside the Jest process
-   integration execution instrumented with external tooling

---

### Running Tests

Requirements:

-   Docker
-   Node.js >= 24

Run locally:

```bash
npm test
```

You can also run the coverage report this way:

```bash
npm run test:coverage
```

The same tests are executed in CI using GitHub Actions

---

## 🧭 Navigation

-   [⬅️ Previous: Advanced Topics](/doc/05_advanced_topics.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Performance](/doc/07_performance.md)
