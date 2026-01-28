import {
  jest,
  describe,
  beforeAll,
  afterAll,
  test,
  expect,
} from '@jest/globals';
import request from 'supertest';
import { GenericContainer, Wait } from 'testcontainers';
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
} from '@aws-sdk/client-s3';
import pg from 'pg';

const { Client } = pg;

jest.setTimeout(120_000);

describe('FDA API - integration', () => {
  let minio, mongo, postgis;
  let minioHostPort; // host:port for DuckDB
  let minioUrl; // http://host:port for AWS SDK
  let mongoUri; // host:port
  let pgHost, pgPort;

  let app;

  beforeAll(async () => {
    // --- MinIO ---
    minio = await new GenericContainer('minio/minio:latest')
      .withEnvironment({
        MINIO_ROOT_USER: 'admin',
        MINIO_ROOT_PASSWORD: 'admin123',
      })
      .withCommand(['server', '/data', '--console-address', ':9001'])
      .withExposedPorts(9000, 9001)
      .withWaitStrategy(Wait.forLogMessage(/API: http:\/\/.*:9000/))
      .start();

    const minioHost = minio.getHost();
    const minioPort = minio.getMappedPort(9000);
    minioHostPort = `${minioHost}:${minioPort}`;
    minioUrl = `http://${minioHostPort}`;

    // --- Mongo ---
    mongo = await new GenericContainer('mongo:8.0')
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();

    mongoUri = `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}`;

    // --- PostGIS ---
    postgis = await new GenericContainer('postgis/postgis:15-3.3')
      .withEnvironment({
        POSTGRES_USER: 'postgres',
        POSTGRES_PASSWORD: 'postgres',
        POSTGRES_DB: 'postgres',
      })
      .withExposedPorts(5432)
      .withWaitStrategy(
        Wait.forLogMessage(/database system is ready to accept connections/)
      )
      .start();

    pgHost = postgis.getHost();
    pgPort = postgis.getMappedPort(5432);

    // Seed Postgres with simple table
    const pgClient = new Client({
      host: pgHost,
      port: pgPort,
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
    });
    await pgClient.connect();

    await pgClient.query(`CREATE SCHEMA IF NOT EXISTS public;`);
    await pgClient.query(`DROP TABLE IF EXISTS public.users;`);
    await pgClient.query(`
      CREATE TABLE public.users (
        id INT PRIMARY KEY,
        name TEXT,
        age INT
      );
    `);
    await pgClient.query(`
      INSERT INTO public.users (id, name, age)
      VALUES (1,'ana',30), (2,'bob',20), (3,'carlos',40);
    `);

    await pgClient.end();

    // Create bucket in MinIO: the bucket is "service" (Fiware-Service)
    const s3 = new S3Client({
      endpoint: minioUrl,
      region: 'us-east-1',
      credentials: { accessKeyId: 'admin', secretAccessKey: 'admin123' },
      forcePathStyle: true,
    });

    const bucket = 'myservice';
    try {
      await s3.send(new HeadBucketCommand({ Bucket: bucket }));
    } catch {
      await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    }

    process.env.FDA_PG_USER = 'postgres';
    process.env.FDA_PG_PASSWORD = 'postgres';
    process.env.FDA_PG_HOST = pgHost;
    process.env.FDA_PG_PORT = String(pgPort);
    process.env.FDA_PG_DB = 'postgres';

    process.env.FDA_OBJSTG_USER = 'admin';
    process.env.FDA_OBJSTG_PASSWORD = 'admin123';
    process.env.FDA_OBJSTG_ENDPOINT = minioHostPort; // DuckDB
    process.env.FDA_MONGO_URI = mongoUri;
    process.env.FDA_NODE_ENV = 'test';

    ({ app } = await import('../../index.js'));
  });

  afterAll(async () => {
    await Promise.allSettled([minio?.stop(), mongo?.stop(), postgis?.stop()]);
  });

  test('POST /fdas creates an FDA and uploads CSV->Parquet to MinIO', async () => {
    const res = await request(app)
      .post('/fdas')
      .set('Fiware-Service', 'myservice')
      .send({
        id: 'fda1',
        database: 'postgres',
        schema: 'public',
        table: 'users',
        path: '/datasets/users',
        description: 'users dataset',
      });

    expect(res.status).toBe(201);
  });

  test('GET /fdas return list of service', async () => {
    const res = await request(app)
      .get('/fdas')
      .set('Fiware-Service', 'myservice');

    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body.some((x) => x.fdaId === 'fda1')).toBe(true);
  });

  test('POST /fdas/:fdaId/das + GET /query executes DuckDB against Parquet', async () => {
    const daQuery = `
      SELECT id, name, age
      FROM read_parquet('s3://myservice/datasets/users')
      WHERE age > $minAge
      ORDER BY id;
    `;

    const createDa = await request(app)
      .post('/fdas/fda1/das')
      .set('Fiware-Service', 'myservice')
      .send({
        id: 'da1',
        description: 'age filter',
        query: daQuery,
      });

    expect(createDa.status).toBe(201);

    const queryRes = await request(app)
      .get('/query')
      .set('Fiware-Service', 'myservice')
      .query({ fdaId: 'fda1', daId: 'da1', minAge: 25 });

    expect(queryRes.status).toBe(200);
    expect(queryRes.body).toEqual([
      { id: 1, name: 'ana', age: 30 },
      { id: 3, name: 'carlos', age: 40 },
    ]);
  });
});
