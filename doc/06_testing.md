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

External dependencies are started dynamically using **Testcontainers**:

-   **PostgreSQL / PostGIS**
-   **MongoDB**
-   **MinIO (S3-compatible object storage)**

The integration entrypoint runs the shared suite twice: once with the app and fetcher in combined roles, and once with
them in separate processes. Each mode gets its own containers and application processes; tests within a mode reuse that
environment and must clean up resources they create.

Each mode:

-   Starts dedicated containers
-   Uses random host ports
-   Stops the containers and application processes after execution

This guarantees:

-   No dependency on services running on fixed local ports
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
-   PostgreSQL FDA bootstrap SQL, including zero-row queries when the source schema has no usable types
-   CSV parsing with DuckDB, including quoted commas beyond the dialect-sniffing sample
-   multipart upload endpoint validation (`/fdas/upload`) including file-type, size and form-field checks

Integration tests validate:

-   FDA creation (PostgreSQL → CSV → Parquet → MinIO)
-   PostgreSQL FDA creation when the query returns zero columns, exercising the `buildZeroRowQuery` bootstrap path
-   Metadata persistence in MongoDB
-   DuckDB query execution over Parquet files
-   End-to-end API behavior using real data
-   direct FDA fresh execution over PostgreSQL
-   default DA behavior including automatic creation and optional filters
-   multipart upload flow (CSV/XLS/XLSX) from API ingestion to parquet generation and queryability

---

### Coverage Considerations

Coverage in this repository is collected with `c8` in two independent runs:

-   unit suite (`coverage/unit/lcov.info`)
-   integration suite (`coverage/integration/lcov.info`)

Then both reports are merged into `coverage/lcov.info`, which is the file uploaded to Coveralls in CI. This flow avoids
ambiguity when reading coverage locally and matches exactly what CI publishes. The merge is additive for the same
file/line in both reports.

-   If a line is covered only by unit tests, it remains covered in the merged report.
-   If a line is covered only by integration tests, it remains covered in the merged report.
-   A line is not covered in the final report only if it is uncovered in both reports.

### Integration Suite Organization

Current integration structure:

-   `test/integration/fda.proc.int.test.js`: entrypoint that runs the same suite in both execution modes
-   `test/integration/fda.integration.shared.js`: shared runtime/bootstrap and suite registration
-   `test/integration/suites/platform.integration.tests.js` and `datasources.integration.tests.js`: platform endpoints
    and datasource management
-   `test/integration/suites/fdaCreation.integration.tests.js`, `fdaVariants.integration.tests.js`,
    `fdaTimeColumn.integration.tests.js`, `fdaLifecycle.integration.tests.js`, `fdaUpdate.integration.tests.js` and
    `lazyFdas.integration.tests.js`: FDA creation, configuration, updates and lifecycle
-   `test/integration/suites/defaultDataAccess.integration.tests.js`, `daDataQueries.integration.tests.js`,
    `daParams.integration.tests.js`, `daCrud.integration.tests.js`, `queryStyleData.integration.tests.js` and
    `freshQueries.integration.tests.js`: DA behavior and query execution
-   `test/integration/suites/slidingWindows.integration.tests.js`, `mongoSlidingWindows.integration.tests.js`,
    `mongoFdas.integration.tests.js` and `complexCases.integration.tests.js`: PostgreSQL/MongoDB materialization,
    windows and combined scenarios
-   `test/integration/suites/uploadFdas.integration.tests.js`, `storageCleanup.integration.tests.js` and
    `storageQuota.integration.tests.js`: uploads and object-storage behavior
-   `test/integration/suites/visibilityConstraints.integration.tests.js`, `cdaCompatibility.integration.tests.js`,
    `downloadLimits.integration.tests.js` and `fdaAccounting.integration.tests.js`: API compatibility and
    resource/accounting constraints

---

### Running Tests

Requirements:

-   Docker
-   Node.js >= 24

Run locally:

```bash
npm test
```

Filter by name or describe:

```bash
npm run test -- -t "Complex FDA queries"
```

Run only the unit or integration suite:

```bash
npm test -- test/unit
npm test -- test/integration
npm run test:performance
```

The unit suite does not require Docker. The integration suite requires Docker and runs the shared API scenarios in both
combined and separated application/fetcher modes. The performance suite is run separately from the default test command.

Run the PostgreSQL zero-column bootstrap integration case in both execution modes:

```bash
npm test -- test/integration/fda.proc.int.test.js -t "bootstraps a PostgreSQL query with no output columns"
```

Run the focused unit tests for the PostgreSQL zero-row query builder and the real-DuckDB CSV dialect case:

```bash
npm test -- test/unit/fda.test.js -t "zero-row PostgreSQL query"
npm test -- test/unit/parquetExport.test.js -t "read_csv_auto parses a quoted comma"
```

Collect coverage for all test suites:

```bash
npm run test:coverage
```

Collect unit and integration coverage separately, then merge the LCOV reports:

```bash
npm run test:coverage:unit
npm run test:coverage:integration
npm run test:coverage:merge
```

The unit coverage command does not require Docker; integration coverage does.

To reproduce the CI coverage flow locally (unit + integration + merged lcov for Coveralls):

```bash
npm run test:coverage:ci
```

The same tests are executed in CI using GitHub Actions.

For the complete list of project npm commands (development, linting, tests, coverage and OpenAPI), see
[`08_developers_guide.md`](./08_developers_guide.md#available-npm-scripts).

---

## 🧭 Navigation

-   [⬅️ Previous: Advanced Topics](/doc/05_advanced_topics.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Performance](/doc/07_performance.md)
