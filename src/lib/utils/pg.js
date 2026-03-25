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

import pg from 'pg';
import { promisify } from 'node:util';
import { to as copyTo } from 'pg-copy-streams';
import Cursor from 'pg-cursor';
import { newUpload } from './aws.js';
import { config } from '../fdaConfig.js';
import { FDAError } from '../fdaError.js';
import { getBasicLogger } from './logger.js';

const { Client, Pool } = pg;
const logger = getBasicLogger();
const pools = new Map();

function getPoolKey(user, host, port, database) {
  return `${user}@${host}:${port}/${database}`;
}

export function getPgPool(user, password, host, port, database) {
  const key = getPoolKey(user, host, port, database);

  if (pools.has(key)) {
    return pools.get(key);
  }

  const pool = new Pool({
    user,
    password,
    host,
    port,
    database,
    max: config.pg.pool.max,
    min: 0,
    idleTimeoutMillis: config.pg.pool.idleTimeoutMillis,
    connectionTimeoutMillis: config.pg.pool.connectionTimeoutMillis,
  });

  pool.on('error', (err) => {
    logger.error({ err, database }, 'PostgreSQL pool error');
  });

  pools.set(key, pool);
  return pool;
}

function releasePgClient(pgClient) {
  if (!pgClient || typeof pgClient.release !== 'function') {
    return;
  }

  try {
    pgClient.release();
  } catch {
    // Ignore release errors to avoid masking the original operation error.
  }
}

export async function closePgPools() {
  const closePromises = [];

  for (const pool of pools.values()) {
    closePromises.push(pool.end().catch(() => {}));
  }

  await Promise.all(closePromises);
  pools.clear();
}

export function getPgClient(user, password, host, port, database) {
  return new Client({
    user,
    password,
    host,
    port,
    database,
  });
}

export async function uploadTable(s3Client, bucket, database, query, path) {
  logger.debug({ bucket, database, query, path }, '[DEBUG]: uploadTable');
  const pgPool = getPgPool(
    config.pg.usr,
    config.pg.pass,
    config.pg.host,
    config.pg.port,
    database,
  );
  const pgClient = await pgPool.connect();

  const baseQuery = `COPY (${query}) TO STDOUT WITH CSV HEADER`;
  const pgStream = pgClient.query(copyTo(baseQuery));

  const parallelUploads3 = newUpload(
    s3Client,
    bucket,
    `${path}.csv`,
    pgStream,
    25,
    1,
  );

  parallelUploads3.on('httpUploadProgress', (progress) => {
    logger.info(progress, 'Uploading table');
  });

  try {
    await parallelUploads3.done();
    logger.debug('Upload completed successfully');
  } catch (e) {
    pgStream.destroy(e);
    throw new FDAError(
      503,
      'UploadError',
      `Error uploading FDA to object storage: ${e.message}`,
    );
  } finally {
    pgStream.destroy();
    releasePgClient(pgClient);
  }
}

export async function runPgQuery(database, text, values) {
  const pgPool = getPgPool(
    config.pg.usr,
    config.pg.pass,
    config.pg.host,
    config.pg.port,
    database,
  );

  const pgClient = await pgPool.connect();

  try {
    const result = await pgClient.query(text, values);
    return result.rows;
  } catch (e) {
    if (e instanceof FDAError) {
      throw e;
    }

    throw new FDAError(
      500,
      'PostgresServerError',
      `Error running fresh query: ${e.message}`,
    );
  } finally {
    releasePgClient(pgClient);
  }
}

export async function createPgCursorReader(database, text, values, batchSize) {
  const pgPool = getPgPool(
    config.pg.usr,
    config.pg.pass,
    config.pg.host,
    config.pg.port,
    database,
  );

  let pgClient;
  let cursor;
  let cleaned = false;

  const close = async () => {
    if (cleaned) {
      return;
    }

    cleaned = true;

    try {
      if (cursor) {
        const closeCursor = promisify(cursor.close.bind(cursor));
        await closeCursor().catch(() => {});
      }
    } finally {
      releasePgClient(pgClient);
    }
  };

  try {
    pgClient = await pgPool.connect();
    cursor = pgClient.query(new Cursor(text, values));
    const readCursor = promisify(cursor.read.bind(cursor));

    return {
      readNextChunk: () => readCursor(batchSize),
      close,
    };
  } catch (e) {
    await close();

    if (e instanceof FDAError) {
      throw e;
    }

    throw new FDAError(
      500,
      'PostgresServerError',
      `Error streaming fresh query: ${e.message}`,
    );
  }
}
