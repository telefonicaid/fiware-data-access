// Copyright 2025 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefónica Soluciones
// de Informática y Comunicaciones de España, S.A.U (hereinafter TSOL) and is protected
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

import { GenericContainer, StartedTestContainer } from 'testcontainers';
import { MongoDBContainer } from '@testcontainers/mongodb';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { MinioContainer } from 'testcontainers';
import path from 'path';
import { spawn } from 'child_process';

let apiProcess;

export default async () => {
  // --- 1. MongoDB ---
  const mongo = await new MongoDBContainer('mongo:6').start();
  process.env.MONGO_URL = mongo.getConnectionString();

  // --- 2. PostgreSQL ---
  const pg = await new PostgreSqlContainer('postgres:16')
    .withDatabase('testdb')
    .withUsername('postgres')
    .withPassword('postgres')
    .start();

  process.env.PG_HOST = pg.getHost();
  process.env.PG_PORT = pg.getPort().toString();
  process.env.PG_DB = pg.getDatabase();
  process.env.PG_USER = pg.getUsername();
  process.env.PG_PASSWORD = pg.getPassword();

  // --- 3. MinIO ---
  const minio = await new GenericContainer('minio/minio')
    .withExposedPorts(9000)
    .withEnv('MINIO_ROOT_USER', 'admin')
    .withEnv('MINIO_ROOT_PASSWORD', 'admin123')
    .withCommand(['server', '/data'])
    .start();

  const minioPort = minio.getMappedPort(9000);
  process.env.MINIO_ENDPOINT = `http://localhost:${minioPort}`;

  // --- 4. Levantar API ---
  apiProcess = spawn('node', ['src/index.js'], {
    env: { ...process.env },
    stdio: 'inherit',
  });

  // Esperar 1s a que arranque el servidor
  await new Promise((res) => setTimeout(res, 1000));

  global.__containers = { mongo, pg, minio, apiProcess };
};
