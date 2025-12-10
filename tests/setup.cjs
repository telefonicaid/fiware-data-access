// Copyright 2025 TelefÃ³nica Soluciones de InformÃ¡tica y Comunicaciones de EspaÃ±a, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by TelefÃ³nica Soluciones
// de InformÃ¡tica y Comunicaciones de EspaÃ±a, S.A.U (hereinafter TSOL) and is protected
// as copyright by the applicable legislation on intellectual property.
//
// It belongs to TSOL, and / or its licensors, the exclusive rights of reproduction,
// distribution, public communication and transformation, and any economic right on it,
// all without prejudice of the moral rights of the authors mentioned above. It is expressly
// forbidden to decompile, disassemble, reverse engineer, sublicense or otherwise transmit
// by any means, translate or create derivative works of the software and / or computer
// programs, and perform with respect to all or part of such programs, any type of exploitation.
//
// Any use of all or part of the software and / or computer program will require the
// express written consent of TSOL. In all cases, it will be necessary to make
// an express reference to TSOL ownership in the software and / or computer
// program.
//
// Non-fulfillment of the provisions set forth herein and, in general, any violation of
// the peaceful possession and ownership of these rights will be prosecuted by the means
// provided in both Spanish and international law. TSOL reserves any civil or
// criminal actions it may exercise to protect its rights.

const { GenericContainer } = require("testcontainers");
const { MongoDBContainer } = require("@testcontainers/mongodb");
const { PostgreSqlContainer } = require("@testcontainers/postgresql");
const { MinioContainer } = require("@testcontainers/minio");
const path = require("path");
const { spawn } = require("child_process");

let apiProcess;

module.exports = async () => {
  console.log("Init Testcontainers environment...");

  // --- 1. MongoDB ---
  const mongo = await new MongoDBContainer("mongo:8.0").start();
  process.env.MONGO_URL = mongo.getConnectionString();
  console.log("   MongoDB started");

  // --- 2. PostgreSQL ---
  const pg = await new PostgreSqlContainer("postgres:16")  
    //const pg = await new PostgreSqlContainer("postgis:15-3.3")
        .withDatabase("testdb")
        .withUsername("postgres")
        .withPassword("postgres")
        .start();

  process.env.PG_HOST = pg.getHost();
  process.env.PG_PORT = String(pg.getPort());
  process.env.PG_DB = pg.getDatabase();
  process.env.PG_USER = pg.getUsername();
  process.env.PG_PASSWORD = pg.getPassword();
  console.log("   PostgreSQL started");

  // --- 3. MinIO ---
  const minio = await new MinioContainer("minio/minio:latest")
        .withUsername("admin")
        .withPassword("admin123")
        .start();

  process.env.MINIO_ENDPOINT = `http://${minio.getHost()}:${minio.getPort()}`;
  const minioPort = minio.getPort(9000);
  console.log("   MinIO started on port", minioPort);

  // --- 4. FDA API ---
  apiProcess = spawn("node", ["./index.js"], {
    env: { ...process.env },
    stdio: "inherit",
  });

  console.log("   API launched, waiting to start...");

  // Wait for service
  await new Promise((res) => setTimeout(res, 1000));

  global.__containers = { mongo, pg, minio, apiProcess };

  console.log(" Test environment ready to work");
};

