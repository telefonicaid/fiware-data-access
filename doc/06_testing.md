## Test Strategy

This project relies on **end-to-end integration tests** rather than unit tests or mocks.

The goal is to validate the full data flow across all external dependencies exactly as it runs in production.

---

### Approach

-   Tests are written using **Jest**
-   The API is exercised using **real HTTP requests** (`supertest`)
-   No external services are mocked

Instead, tests start **real service instances** using Docker.

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

Integration tests validate:

-   FDA creation (PostgreSQL → CSV → Parquet → MinIO)
-   Metadata persistence in MongoDB
-   DuckDB query execution over Parquet files
-   End-to-end API behavior using real data

---

### Why No Unit Tests

Unit tests and mocks are intentionally avoided because:

-   The core complexity lies in **integration between systems**
-   Mocking databases, S3, or DuckDB would not validate real behavior
-   The cost of integration testing is acceptable for this service

---

### Coverage Considerations

Because the application runs as a **separate process**, standard Jest coverage:

-   Does not instrument the application process
-   Only measures the test runner itself

Coverage can be collected using additional tooling (e.g. `c8`) if required, but is not enforced by default.

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
-   [➡️ Next: Testing](/doc/07_performance.md)
