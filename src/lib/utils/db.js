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

import { retrieveDA } from './mongo.js';
import { FDAError } from '../fdaError.js';
import { getBasicLogger } from './logger.js';
import { config } from '../fdaConfig.js';

let instance = null;

// key: `${service}::${fdaId}` -> Map(daId -> sql query)
const cachedQueries = new Map();
const logger = getBasicLogger();

const connectionPool = [];
const MAX_POOL_SIZE = 10;

export async function releaseDBConnection(conn) {
  if (connectionPool.length < MAX_POOL_SIZE) {
    // Reset connection before return to pool
    try {
      await conn.run('RESET ALL;');
      await configureConn(conn);
      connectionPool.push(conn);
    } catch (error) {
      // If error close connection
      await closeConnection(conn);
    }
  } else {
    // Pool full, close connection
    await closeConnection(conn);
  }
}

export async function getDBConnection() {
  await initDuckDB();

  // Reuse connection from pool if exists
  if (connectionPool.length > 0) {
    return connectionPool.pop();
  }

  // Create a new connection if pool is empty
  const conn = await instance.connect();
  await configureConn(conn);

  return conn;
}

async function initDuckDB() {
  if (!instance) {
    logger.debug('Initializing DuckDB global instance...');
    // Lazy import: avoid  "module is already linked" in Jest ESM/VM
    const { DuckDBInstance } = await import('@duckdb/node-api');
    instance = await DuckDBInstance.create(':memory:');

    // Init connection for config
    const configConn = await instance.connect();
    await configConn.run('INSTALL httpfs;');
    await configConn.run('LOAD httpfs;');

    // Common config
    await configConn.run(`
      SET s3_endpoint='${config.objstg.endpoint}';
      SET s3_url_style='path';
      SET s3_use_ssl=false;
      SET s3_access_key_id='${config.objstg.usr}';
      SET s3_secret_access_key='${config.objstg.pass}';
    `);

    if (typeof configConn.disconnect === 'function') {
      await configConn.disconnect();
    }
    logger.debug('DuckDB initialized with HTTPFS');
  }
  return instance;
}

export async function runPreparedStatement(conn, service, fdaId, daId, params) {
  logger.debug(
    { service, fdaId, daId, params },
    '[DEBUG]: runPreparedStatement',
  );

  let query = getCachedQuery(service, fdaId, daId);

  if (!query) {
    const da = await retrieveDA(service, fdaId, daId);
    if (!da?.query) {
      throw new FDAError(
        404,
        'DaNotFound',
        `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
      );
    }
    query = da.query;
    await storeCachedQuery(service, fdaId, daId, query);
  }

  const stmt = await conn.prepare(query);

  try {
    await stmt.bind(params);
    const result = await stmt.run();
    return result.getRowObjectsJson();
  } catch (e) {
    throw new FDAError(
      500,
      'DuckDBServerError',
      `Error running the prepared statement: ${e}`,
    );
  } finally {
    // release statement resources
    if (typeof stmt.close === 'function') {
      await stmt.close();
    }
  }
}

export async function runPreparedStatementStream(
  conn,
  service,
  fdaId,
  daId,
  params,
) {
  logger.debug(
    { service, fdaId, daId, params },
    '[DEBUG]: runPreparedStatementStream',
  );

  let query = getCachedQuery(service, fdaId, daId);

  if (!query) {
    const da = await retrieveDA(service, fdaId, daId);
    if (!da?.query) {
      throw new FDAError(
        404,
        'DaNotFound',
        `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
      );
    }
    query = da.query;
    await storeCachedQuery(service, fdaId, daId, query);
  }

  const stmt = await conn.prepare(query);

  try {
    await stmt.bind(params);
    const stream = await stmt.stream();

    const close = async () => {
      if (typeof stmt.close === 'function') {
        await stmt.close();
      }
    };
    return { stream, close };
  } catch (e) {
    if (typeof stmt.close === 'function') {
      await stmt.close();
    }
    throw new FDAError(
      500,
      'DuckDBServerError',
      `Error streaming the prepared statement: ${e}`,
    );
  }
}

function fdaKey(service, fdaId) {
  return `${service}::${fdaId}`;
}
function getCachedQuery(service, fdaId, daId) {
  return cachedQueries.get(fdaKey(service, fdaId))?.get(daId);
}

function storeCachedQuery(service, fdaId, daId, query) {
  const key = fdaKey(service, fdaId);
  let fda = cachedQueries.get(key);

  if (!fda) {
    fda = new Map();
    cachedQueries.set(key, fda);
  }

  fda.set(daId, query);
}

export function toParquet(conn, originPath, resultPath) {
  logger.debug({ originPath, resultPath }, '[DEBUG]: toParquet');
  return conn.run(
    `COPY ( SELECT * FROM read_csv_auto('s3://${originPath}')) 
    TO 's3://${resultPath}' (FORMAT PARQUET);`,
  );
}

async function configureConn(conn) {
  await conn.run('LOAD httpfs;');
  await conn.run(`
    SET s3_endpoint='${config.objstg.endpoint}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='${config.objstg.usr}';
    SET s3_secret_access_key='${config.objstg.pass}';
  `);
}

async function closeConnection(conn) {
  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }
}
