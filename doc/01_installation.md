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

## Common issues

-   **MinIO bucket not found** Ensure the bucket exists before creating FDAs. When using Docker Compose, this is handled
    automatically by the `mc` service.

-   **PostgreSQL connection errors** Verify `FDA_PG_HOST`, `FDA_PG_PORT`, and credentials.

-   **Wrong Node.js version** Ensure Node.js 24 is being used when running locally.

---

## 🧭 Navigation

-   [⬅️ Previous: Overview](/doc/00_overview.md)
-   [🏠 Main index](../README.md#documentation)
-   [➡️ Next: Architecture](/doc/02_architecture.md)
