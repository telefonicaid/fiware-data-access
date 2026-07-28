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
pg.types.setTypeParser(pg.types.builtins.INT8, (value) =>
  value === null ? null : BigInt(value),
);
const logger = getBasicLogger();
const pools = new Map();

function getPoolKey(username, password, host, port, database) {
  return JSON.stringify({ username, password, host, port, database });
}

function clearPoolIdleTimer(entry) {
  if (!entry?.idleTimer) {
    return;
  }

  clearTimeout(entry.idleTimer);
  entry.idleTimer = null;
}

function schedulePoolIdleClose(key) {
  const timeoutMs = Number(config.pg.pool.databaseIdleTimeoutMillis);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return;
  }

  const entry = pools.get(key);
  if (!entry) {
    return;
  }

  clearPoolIdleTimer(entry);

  entry.idleTimer = setTimeout(async () => {
    const current = pools.get(key);
    if (!current) {
      return;
    }

    const { pool } = current;
    const allIdle =
      pool.totalCount === pool.idleCount && pool.waitingCount === 0;

    if (!allIdle) {
      // Keep checking while there is active or queued work.
      schedulePoolIdleClose(key);
      return;
    }

    pools.delete(key);
    clearPoolIdleTimer(current);
    await pool.end().catch(() => {});
  }, timeoutMs);
}

export function getPgPool(username, password, host, port, database) {
  const key = getPoolKey(username, password, host, port, database);

  if (pools.has(key)) {
    const existing = pools.get(key);
    clearPoolIdleTimer(existing);
    return existing.pool;
  }

  const pool = new Pool({
    user: username,
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

  pools.set(key, { pool, idleTimer: null });
  return pool;
}

function releasePgClient(key, pgClient) {
  if (!pgClient || typeof pgClient.release !== 'function') {
    return;
  }

  try {
    pgClient.release();
    schedulePoolIdleClose(key);
  } catch {
    // Ignore release errors to avoid masking the original operation error.
  }
}

export function getDuckDBTypeFromPostgresField(field) {
  const typeId = field?.dataTypeID;

  switch (typeId) {
    case pg.types.builtins.BOOL:
      return 'BOOLEAN';
    case pg.types.builtins.INT2:
      return 'SMALLINT';
    case pg.types.builtins.INT4:
      return 'INTEGER';
    case pg.types.builtins.INT8:
      return 'BIGINT';
    case pg.types.builtins.FLOAT4:
      return 'REAL';
    case pg.types.builtins.FLOAT8:
      return 'DOUBLE';
    case pg.types.builtins.NUMERIC:
      return 'DOUBLE';
    case pg.types.builtins.DATE:
      return 'DATE';
    case pg.types.builtins.TIME:
    case pg.types.builtins.TIMETZ:
      return 'TIME';
    case pg.types.builtins.TIMESTAMP:
      return 'TIMESTAMP';
    case pg.types.builtins.TIMESTAMPTZ:
      return 'TIMESTAMPTZ';
    case pg.types.builtins.JSON:
    case pg.types.builtins.JSONB:
      return 'JSON';
    case pg.types.builtins.UUID:
      return 'UUID';
    case pg.types.builtins.BYTEA:
      return 'BLOB';
    default:
      return 'VARCHAR';
  }
}

function buildPostgresSchemaInfo(result) {
  const fields = Array.isArray(result?.fields)
    ? result.fields
        .map((field) => {
          const name = field?.name;
          if (typeof name !== 'string' || name.length === 0) {
            return null;
          }

          return {
            name,
            postgresTypeId: field?.dataTypeID ?? null,
            duckdbType: getDuckDBTypeFromPostgresField(field),
          };
        })
        .filter(Boolean)
    : [];

  return {
    columns: fields.map((field) => field.name),
    fields,
  };
}

export async function closePgPools() {
  const closePromises = [];

  for (const entry of pools.values()) {
    clearPoolIdleTimer(entry);
    closePromises.push(entry.pool.end().catch(() => {}));
  }

  await Promise.all(closePromises);
  pools.clear();
}

export function getPgClient(username, password, host, port, database) {
  return new Client({
    user: username,
    password,
    host,
    port,
    database,
  });
}

export async function uploadTable(
  s3Client,
  bucket,
  pgCredentials,
  query,
  path,
) {
  const { username, password, host, port, database } = pgCredentials;
  logger.debug({ bucket, database, query, path }, '[DEBUG]: uploadTable');
  const key = getPoolKey(username, password, host, port, database);
  const pgPool = getPgPool(username, password, host, port, database);
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
    releasePgClient(key, pgClient);
  }
}

export async function runPgQuery(pgCredentials, text, values) {
  const { username, password, host, port, database } = pgCredentials;
  const key = getPoolKey(username, password, host, port, database);
  const pgPool = getPgPool(username, password, host, port, database);

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
    releasePgClient(key, pgClient);
  }
}

const TEMPORAL_TYPES = new Set([
  'TIMESTAMP',
  'TIMESTAMPTZ',
  'DATE',
  'TIME',
  'TIMESTAMP WITHOUT TIME ZONE',
  'TIMESTAMP WITH TIME ZONE',
]);

export async function validatePostgresQuery(
  pgCredentials,
  query,
  { timeColumn, returnColumns = false } = {},
) {
  const { username, password, host, port, database } = pgCredentials;
  const key = getPoolKey(username, password, host, port, database);
  const pgPool = getPgPool(username, password, host, port, database);
  const pgClient = await pgPool.connect();

  const normalizedQuery = query.trim().replace(/;+\s*$/, '');
  const validationQuery = `SELECT * FROM (${normalizedQuery}) AS fda_validation LIMIT 0`;

  try {
    const result = await pgClient.query(validationQuery);
    const schemaInfo = buildPostgresSchemaInfo(result);
    const { fields } = schemaInfo;

    if (typeof timeColumn === 'string' && timeColumn.length > 0) {
      const columnInfo = fields.find(
        (field) => field.name.toLowerCase() === timeColumn.toLowerCase(),
      );

      if (!columnInfo) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Time column "${timeColumn}" is not present in the SELECT clause of the FDA query. `,
        );
      }

      if (!TEMPORAL_TYPES.has(columnInfo.duckdbType.toUpperCase())) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Time column "${timeColumn}" must be of a temporal type (TIMESTAMP, TIMESTAMPTZ, DATE or TIME). ` +
            `Got "${columnInfo.duckdbType}" instead.`,
        );
      }
    }

    return returnColumns ? schemaInfo : null;
  } catch (e) {
    if (e instanceof FDAError) {
      throw e;
    }

    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid Postgres FDA query: ${e.message}`,
    );
  } finally {
    releasePgClient(key, pgClient);
  }
}

export async function createPgCursorReader(
  pgCredentials,
  text,
  values,
  batchSize,
) {
  const { username, password, host, port, database } = pgCredentials;
  const key = getPoolKey(username, password, host, port, database);
  const pgPool = getPgPool(username, password, host, port, database);

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
      releasePgClient(key, pgClient);
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

export async function validatePostgresDatasourceConnection(dsConfig) {
  try {
    await runPgQuery(dsConfig, 'SELECT 1', []);
  } catch (error) {
    throw new FDAError(
      400,
      'InvalidDatasourceConnection',
      `Could not connect to datasource: ${error.message}`,
    );
  }
}
