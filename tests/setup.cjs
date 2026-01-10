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


const { MongoDBContainer } = require("@testcontainers/mongodb");
const { PostgreSqlContainer } = require("@testcontainers/postgresql");
const { MinioContainer } = require("@testcontainers/minio");
const { spawn } = require("child_process");
const { MongoClient } = require("mongodb");
const { Client } = require("pg");
const { S3Client, PutObjectCommand, CreateBucketCommand } = require("@aws-sdk/client-s3");

let apiProcess;

// --- Helper functions to data init ---
async function seedMongo(mongo) {
  const host = mongo.getHost();
  const port = mongo.getMappedPort(27017);
  const mongoUrl = `mongodb://${host}:${port}`;
  console.log("Seeding Mongo at", mongoUrl);
  const client = new MongoClient(mongoUrl);
  await client.connect();
  const db = client.db("fiware-data-access");
  const sets = db.collection("sets");

  await sets.insertOne({
    _id: "set1",
    table: "mytable",
    bucket: "bucket-test",
    path: "path-test",
    queries: {},
    service: "test",
  });

  await client.close();
}

async function seedPostgres({ host, port, user, password, database }) {
  const client = new Client({ host, port, user, password, database });
  await client.connect();

  await client.query(`
    CREATE TABLE IF NOT EXISTS mytable (
      id SERIAL PRIMARY KEY,
      name TEXT
    );
  `);

  await client.query(`
    INSERT INTO mytable (name) VALUES ('Alice'), ('Bob');
  `);

  await client.end();
}

async function seedMinio(endpoint, accessKey, secretKey, bucket) {
  const s3 = new S3Client({
    endpoint,
    region: "us-east-1",
    credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
    forcePathStyle: true,
  });

  // Create Bucket if exists
  try {
    await s3.send(new CreateBucketCommand({ Bucket: bucket }));
  } catch (e) {
    if (!e.message.includes("BucketAlreadyOwnedByYou")) {
      console.error("Error creating bucket:", e);
      throw e;
    }
  }

  // Upload a CSV example file
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: "path-test/sample.csv",
      Body: "id,name\n1,Alice\n2,Bob\n",
    })
  );
}

module.exports = async () => {
  console.log("Init Testcontainers environment...");

  // --- 1. MongoDB ---
  const mongo = await new MongoDBContainer("mongo:8.0").start();
  const host = mongo.getHost();
  const port = mongo.getMappedPort(27017);
  process.env.MONGO_URL = `mongodb://${host}:${port}`;
  console.log("MongoDB started at", process.env.MONGO_URL);
  //await seedMongo(mongo);

  // --- 2. PostgreSQL ---
  const pg = await new PostgreSqlContainer("postgres:16")  
    //const pg = await new PostgreSqlContainer("postgis:15-3.3")
        .withDatabase("testdb")
        .withUsername("postgres")
        .withPassword("postgres")
        .start();
  await new Promise((res) => setTimeout(res, 3000));
  process.env.PG_HOST = pg.getHost();
  process.env.PG_PORT = String(pg.getPort());
  process.env.PG_DB = pg.getDatabase();
  process.env.PG_USER = pg.getUsername();
  process.env.PG_PASSWORD = pg.getPassword();
  process.env.PG_DATABASE = pg.getDatabase();
    console.log("   PostgreSQL started on ", pg.getHost(),pg.getPort(),pg.getDatabase()  );
  await seedPostgres({
    host: pg.getHost(),
    port: pg.getPort(),
    user: pg.getUsername(),
    password: pg.getPassword(),
    database: pg.getDatabase(),
  });

  // --- 3. MinIO ---
  const minio = await new MinioContainer("minio/minio:latest")
        .withUsername("admin")
        .withPassword("admin123")
        .start();

  process.env.MINIO_ENDPOINT = `http://${minio.getHost()}:${minio.getPort()}`;
  const minioPort = minio.getPort(9000);
  console.log("   MinIO started on port", minioPort);
  await seedMinio(
    process.env.MINIO_ENDPOINT,
    "admin",
    "admin123",
    "bucket-test"
  );

  // --- 4. FDA API ---
  // TODO: use a globalconfig to start app to link with started testcontainers hosts/ports
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


