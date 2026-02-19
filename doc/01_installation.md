# Installation and Running

This document describes how to install and run **FIWARE Data Access (FDA)**, both using **Docker Compose** and in a
**local development setup**.

---

## Requirements

Depending on how you want to run the project, you will need:

### Docker-based setup

-   Docker
-   Docker Compose (v2)

### Local development setup

-   Node.js **24.x**
-   npm
-   Docker (to run dependencies: PostgreSQL, MongoDB, MinIO)

---

## Running with Docker Compose (recommended)

The easiest way to run FIWARE Data Access is using **Docker Compose**, which starts all required services:

-   FIWARE Data Access API
-   PostgreSQL (PostGIS)
-   MongoDB
-   MinIO (object storage)
-   MinIO client (`mc`) to initialize the bucket

### Steps

From the **root of the repository**, run:

```bash
docker compose -f docker/docker-compose.yml up
```

This will:

1. Start **MinIO** and expose:
    - API: `http://localhost:9000`
    - Console: `http://localhost:9001`
2. Create the default bucket using the MinIO client
3. Start **MongoDB**
4. Start **PostgreSQL (PostGIS)**
5. Build and start the **FDA** service

Once the process finishes, FDA will be available at:

```text
http://localhost:8080
```

> **Note:** The Docker Compose setup automatically provides default values for enviroment variables, except for
> `FDA_OBJSTG_USER` and `FDA_OBJSTG_PASSWORD`. Ensure that `FDA_OBJSTG_USER` and `FDA_OBJSTG_PASSWORD` are properly
> defined or exported (typically as `${MINIO_ROOT_USER}` and `${MINIO_ROOT_PASSWORD}` respectively).

---

## Docker images and build

The FDA Docker image is built using the following characteristics:

-   Base image: `node:24-slim`
-   Application code located in `/opt/fda/fda`
-   Production dependencies installed with `npm install --production`
-   Default command: `npm start`

The Dockerfile used is located at:

```text
docker/Dockerfile
```

---

## Running in local development mode

For development purposes, it is often useful to run the FDA service locally while keeping dependencies running in
Docker.

### 1. Start infrastructure services

You can start only the required services using Docker:

-   PostgreSQL (PostGIS)
-   MongoDB
-   MinIO

For example, using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up minio mongo postgis
```

Ensure that:

-   PostgreSQL is reachable on port `5432`
-   MongoDB is reachable on port `27017`
-   MinIO is reachable on port `9000`

---

### 2. Use Node.js 24

FDA requires **Node.js 24**.

If you use `nvm`:

```bash
nvm use 24
```

Example output:

```text
Now using node v24.13.0 (npm v11.6.2)
```

---

### 3. Install dependencies

From the root of the repository:

```bash
npm install
```

---

### 4. Configure environment variables

Create a `.env` file (or export variables in your shell) with the required configuration.

At minimum, you will need:

```env
FDA_NODE_ENV=development
FDA_SERVER_PORT=8080

FDA_PG_USER=postgres
FDA_PG_PASSWORD=postgres
FDA_PG_HOST=localhost
FDA_PG_PORT=5432

FDA_OBJSTG_PROTOCOL=http
FDA_OBJSTG_ENDPOINT=localhost:9000
FDA_OBJSTG_USER=admin
FDA_OBJSTG_PASSWORD=admin123

FDA_MONGO_URI=mongodb://localhost:27017
```

Refer to [`04_config_operational_guide.md`](./04_config_operational_guide.md) for the full list of configuration
options.

---

### 5. Start the FDA service

Run:

```bash
npm start
```

You should see the FDA service starting and listening on port `8080`.

---

### 6. Verify the setup with tests

To verify that everything is configured correctly, you can run the test suite:

```bash
npm test
```

This will run the integration tests to ensure all components are working properly.

---

## Complete Working Example (Manual Verification)

This section provides a step-by-step example to validate a complete FDA workflow.

### Prerequisites

-   FDA service running (follow the [Running with Docker Compose](#running-with-docker-compose-recommended) section)
-   curl available in your terminal

### 1. Verify the service is running

Check that FDA is healthy:

```bash
curl -i http://localhost:8080/health
```

Expected response:

```text
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{"status":"UP","timestamp":"2026-02-11T12:51:25.166Z"}
```

### 2. Create a MinIO bucket and PostgreSQL database

Both the bucket and database must have the **same name** and will be referred to as `fiware-service`.

For this example, we'll use `my-bucket`:

#### Create MinIO bucket

Using the MinIO console:

1. Open `http://localhost:9000`
2. Login with credentials from your Docker Compose setup
3. Create a bucket named `my-bucket`

#### Create PostgreSQL database

```bash
# Assuming PostgreSQL is running in Docker
docker exec -it postgres_container psql -U postgres -c 'CREATE DATABASE "my-bucket";'
```

Or directly (if PostgreSQL is on localhost):

```bash
psql -h localhost -U postgres -c 'CREATE DATABASE "my-bucket";'
```

### 3. Create sample data in PostgreSQL

Connect to the `my-bucket` database and create the sample table:

```bash
docker exec -it postgres_container psql -U postgres -d "my-bucket" << 'EOF'
DROP TABLE IF EXISTS public.alarms;

CREATE TABLE public.alarms (
    entityID TEXT PRIMARY KEY,
    entityType TEXT NOT NULL,
    "__ALERTDESCRIPTION__" TEXT,
    "__NAME__" TEXT NOT NULL,
    "__SEVERITY__" TEXT NOT NULL,
    "__TIME_BETWEEN_NOTIF__" INTEGER,
    templateId TEXT,
    "__ATTR__" TEXT,
    "__OPER__" TEXT,
    "__UMBRAL__" NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO public.alarms (
    entityID,
    entityType,
    "__ALERTDESCRIPTION__",
    "__NAME__",
    "__SEVERITY__",
    "__TIME_BETWEEN_NOTIF__",
    templateId,
    "__ATTR__",
    "__OPER__",
    "__UMBRAL__"
) VALUES
-- Medium severity
(
    'alarm_nosignal_001',
    'template',
    'Regla que evalua si llegan medidas',
    'nosignal_001',
    'medium',
    3600000,
    'alarm_nosenal_usuario',
    NULL,
    NULL,
    NULL
),
(
    'alarm_threshold_04',
    'template',
    'Alerta de nivel de llenado por superación de umbral',
    'threshold_04',
    'medium',
    1800000,
    'comparacion_umbral_usuario',
    'fillingLevel',
    '>=',
    0.9
),
-- High severity
(
    'alarm_fire_001',
    'template',
    'Alarma crítica por incendio detectado',
    'fire_001',
    'high',
    60000,
    'fire_template',
    'temperature',
    '>',
    80
),
(
    'alarm_intrusion_01',
    'template',
    'Intrusión detectada en zona restringida',
    'intrusion_01',
    'high',
    120000,
    'intrusion_template',
    'movement',
    '=',
    1
),
-- Low severity
(
    'alarm_battery_low_01',
    'template',
    'Nivel de batería bajo',
    'battery_low_01',
    'low',
    7200000,
    'battery_template',
    'batteryLevel',
    '<',
    20
);

SELECT * FROM public.alarms;
EOF
```

### 4. List existing FDAs

List all FDAs for the service `my-bucket`:

```bash
curl -i -X GET http://localhost:8080/fdas \
  -H "Fiware-Service: my-bucket"
```

Expected response (should be empty initially):

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

[]
```

### 5. Create a new FDA

Create an FDA that extracts all alarms from the PostgreSQL table:

```bash
curl -i -X POST http://localhost:8080/fdas \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -H "Fiware-ServicePath: /public" \
  -d '{
    "id": "fda_alarms",
    "query": "SELECT * FROM public.alarms",
    "description": "FDA de alarmas del sistema"
  }'
```

Expected response:

```text
HTTP/1.1 202 Accepted
Content-Type: application/json; charset=utf-8

{"id":"fda_alarms","status":"pending"}
```

If the response is `202`, the FDA was accepted and it will be created.

### 6. Verify the FDA was created

List FDAs again to confirm:

```bash
curl -i -X GET http://localhost:8080/fdas \
  -H "Fiware-Service: my-bucket"
```

Expected response (should now contain the FDA and his status):

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

[
  {
    "_id": "...",
    "fdaId": "fda_alarms",
    "service": "my-bucket",
    "status":"completed",
    "progress":100,
    "lastExecution":"2026-02-19T07:38:21.263Z",
    "servicePath: /public"
    "query": "SELECT * FROM public.alarms",
    "description": "FDA de alarmas del sistema",
    "das": {}
  }
]
```

> **Note**: to execute queries against an FDA, it will be necesary to has `"status": "completed"`.

### 7. Create a DA (Data Access) for the FDA and run a query

You can create a DA that references the FDA (for example, reading a Parquet file in object storage) and then run a query
against it.

Create the DA for `fda_alarms`:

```bash
curl -i -X POST http://localhost:8080/fdas/fda_alarms/das \
  -H "Content-Type: application/json" \
  -H "Fiware-Service: my-bucket" \
  -d '{
    "id": "da_all_alarms",
    "description": "Todas las alarmas (actualizado)",
    "query": "SELECT * LIMIT 10"
  }'
```

> Avoid using the **FROM** clause within the DA query; it will be automatically added by the system.

Expected response:

```
HTTP/1.1 201 Created
X-Powered-By: Express
Content-Type: text/plain; charset=utf-8
Content-Length: 7
...
Created
```

Run a query against the FDA/DA (JSON response):

```bash
curl -i -X GET "http://localhost:8080/query?fdaId=fda_alarms&daId=da_all_alarms" \
  -H "Fiware-Service: my-bucket"
```

Example JSON response (array):

```json
[{"entityid":"alarm_nosignal_001","entitytype":"template","__ALERTDESCRIPTION__":"Regla que evalua si llegan medidas","__NAME__":"nosignal_001","__SEVERITY__":"medium","__TIME_BETWEEN_NOTIF__":"3600000","templateid":"alarm_nosenal_usuario","__ATTR__":null,"__OPER__":null,"__UMBRAL__":null,"created_at":"2026-02-11 10:41:17.960528"}, ...]
```

Or request streaming NDJSON by setting the `Accept` header:

```bash
curl -i -X GET "http://localhost:8080/query?fdaId=fda_alarms&daId=da_all_alarms" \
  -H "Fiware-Service: my-bucket" \
  -H 'Accept: application/x-ndjson'
```

Example NDJSON streaming output (one JSON object per line):

```
{"entityid":"alarm_nosignal_001","entitytype":"template",...}
{"entityid":"alarm_threshold_04","entitytype":"template",...}
...
```

Notes:

-   Use the same `Fiware-Service` header when creating the DA and when querying.
-   The NDJSON response is useful for streaming large result sets; timestamps may be returned in a structured format
    (e.g. micros).

---

## Common issues

-   **MinIO bucket not found** Ensure the bucket exists before creating FDAs. When using Docker Compose, this is handled
    automatically by the `mc` service. For local development, create the bucket manually (see
    [Complete Working Example](#complete-working-example-manual-verification)).

-   **PostgreSQL connection errors** Verify `FDA_PG_HOST`, `FDA_PG_PORT`, and credentials. Ensure the database with the
    same name as the `fiware-service` exists.

-   **Wrong Node.js version** Ensure Node.js 24 is being used when running locally.

-   **FDA creation fails with 500 error** Ensure:
    1. The MinIO bucket exists with the same name as the `fiware-service` header
    2. The PostgreSQL database exists with the same name as the `fiware-service` header
    3. The `fiware-service` header is provided in all FDA operations

---

## 🧭 Navigation

-   [⬅️ Previous: Overview](/doc/00_overview.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Architecture](/doc/02_architecture.md)
