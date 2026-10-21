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
-   multipart upload endpoint validation (`/fdas/upload`) including file-type, size and form-field checks

Integration tests validate:

-   FDA creation (PostgreSQL → CSV → Parquet → MinIO)
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
-   `test/integration/suites/platform.integration.tests.js`: platform endpoints (`/health`, `/metrics`)
-   `test/integration/suites/fdaCreation.integration.tests.js`: FDA creation and basic lifecycle creation checks
-   `test/integration/suites/slidingWindows.integration.tests.js`: sliding-window and partitioning checks
-   `test/integration/suites/uploadFdas.integration.tests.js`: multipart upload endpoint and upload-processing scenarios

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

You can also run coverage this way:

```bash
npm run test:coverage
```

To reproduce the CI coverage flow locally (unit + integration + merged lcov for Coveralls):

```bash
npm run test:coverage:ci
```

The same tests are executed in CI using GitHub Actions

---

## Available npm Scripts

The following scripts are defined in `package.json` and can be executed with `npm run <script>` (`npm start` and
`npm test` can be used without `run`). Extra arguments can be forwarded to the underlying tool after `--`, for example
`npm test -- test/unit`.

### Development

| Script  | Description                                                                                                                                                                                                |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `start` | Starts the FDA service (`node src/index.js`). Used by the Docker image. Requires the configuration described in [`04_config_operational_guide.md`](./04_config_operational_guide.md) (e.g. a `.env` file). |
| `dev`   | Starts the FDA service with `nodemon`, restarting it automatically when source files change. Intended for local development only. Same configuration requirements as `start`.                              |
| `clean` | Removes `node_modules` and `package-lock.json`. Useful to force a clean reinstall of dependencies; run `npm install` afterwards.                                                                           |

### Linting and Formatting

| Script      | Description                                                                                                                                   |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `lint`      | Runs `lint:api` and `lint:test`. This is the check executed in CI, so run it before opening a pull request.                                   |
| `lint:api`  | Runs ESLint over the application sources (`src/lib/` and `src/routes/`).                                                                      |
| `lint:test` | Runs ESLint over the integration tests (`test/integration/`).                                                                                 |
| `lint:fix`  | Runs ESLint over `src/lib/` and `src/routes/` applying automatic fixes. Test files are not fixed by this script.                              |
| `prettier`  | Formats all JavaScript files of the repository with Prettier using `.prettierrc.json`. Modifies files in place, so review the resulting diff. |

ESLint scripts use `--cache`, storing results in `.eslintcache` (ignored by git). Additionally, a `husky` pre-commit
hook runs `lint-staged`, which formats staged `*.js` and `*.md` files with Prettier automatically.

### Tests

All test scripts run Jest in band (`--runInBand`) with `NODE_ENV=test` and `--experimental-vm-modules` (required for ES
modules). Integration and performance tests require Docker to be running, since dependencies are started with
Testcontainers.

| Script                    | Description                                                                                                                                                                                                                                                                        |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test`                    | Runs every `*.test.js` file of the repository (unit, integration and performance suites). Because the performance suite is included, a full run can take a long time; pass a path or a name filter to narrow it down (e.g. `npm test -- test/unit` or `npm test -- -t "pattern"`). |
| `test:performance`        | Runs only the performance suite (`test/performance`). See [Performance](./07_performance.md) for available parameters.                                                                                                                                                             |
| `test:performance:memory` | Runs the DuckDB window memory scenarios of the performance suite (`FDA_MEMORY_MATRIX=true`) with a 3,000,000-row source table. Demanding in time and memory. See [DuckDB memory management](./AdvancedTopics/duckdb_memory_management.md).                                         |

### Coverage

Coverage is collected with `c8` over `src/**/*.js` and written in `lcov` and text-summary formats. See
[Coverage Considerations](#coverage-considerations) for details on how reports are merged.

| Script                      | Description                                                                                                                                    |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `test:coverage`             | Runs all test suites (same scope as `test`) collecting coverage into `coverage/`.                                                              |
| `test:coverage:unit`        | Runs only the unit tests (`test/unit`) collecting coverage into `coverage/unit/`. Does not require Docker. Executed in CI.                     |
| `test:coverage:integration` | Runs only the integration tests (`test/integration`) collecting coverage into `coverage/integration/`. Requires Docker. Executed in CI.        |
| `test:coverage:merge`       | Merges `coverage/*/lcov.info` into `coverage/lcov.info` using `lcov-result-merger` (downloaded through `npx`, network access may be required). |
| `test:coverage:ci`          | Runs `test:coverage:unit`, `test:coverage:integration` and `test:coverage:merge` in sequence, reproducing locally the report published by CI.  |

### OpenAPI

| Script             | Description                                                                                                                                                                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `openapi:generate` | Generates `openapi.generated.json` (ignored by git) from the `@swagger` JSDoc annotations in `src/routes/*.js` and `src/lib/openapiComponents.js`.                                                                        |
| `openapi:validate` | Runs `openapi:generate` and lints the generated spec with `@redocly/cli` (downloaded through `npx`, network access may be required). Executed in CI; run it after changing API routes or their documentation annotations. |

---

## 🧭 Navigation

-   [⬅️ Previous: Advanced Topics](/doc/05_advanced_topics.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Performance](/doc/07_performance.md)
