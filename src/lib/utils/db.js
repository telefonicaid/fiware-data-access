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

// key: `${service}::${fdaId}` -> Map(daId -> { sql query, params})
const cachedQueries = new Map();
const logger = getBasicLogger();

const connectionPool = [];

export async function releaseDBConnection(conn) {
  if (connectionPool.length < config.objstg.maxPoolSize) {
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

export async function runPreparedStatement(
  conn,
  service,
  fdaId,
  daId,
  paramValues,
) {
  logger.debug(
    { service, fdaId, daId, paramValues: JSON.stringify(paramValues) },
    '[DEBUG]: runPreparedStatement',
  );

  let { query, params } = { ...getCachedQuery(service, fdaId, daId) };

  if (!query) {
    const da = await retrieveDA(service, fdaId, daId);
    if (!da?.query) {
      throw new FDAError(
        404,
        'DaNotFound',
        `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
      );
    }

    query = buildDAQuery(service, fdaId, da.query);
    params = da.params || [];
    await storeCachedQuery(conn, service, fdaId, daId, query, params);
  }

  const stmt = await conn.prepare(query);

  try {
    let boundParams = [];

    if (Array.isArray(params) && params.length > 0) {
      boundParams = applyParams(paramValues, params);

      if (!Array.isArray(boundParams)) {
        throw new FDAError(
          500,
          'InvalidBindParams',
          'applyParams did not return an array',
        );
      }

      await stmt.bind(boundParams);
    }

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
  paramValues,
) {
  logger.debug(
    { service, fdaId, daId, paramValues },
    '[DEBUG]: runPreparedStatementStream',
  );

  let { query, params } = { ...getCachedQuery(service, fdaId, daId) };

  if (!query) {
    const da = await retrieveDA(service, fdaId, daId);
    if (!da?.query) {
      throw new FDAError(
        404,
        'DaNotFound',
        `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
      );
    }
    query = buildDAQuery(service, fdaId, da.query);
    params = da.params;
    await storeCachedQuery(conn, service, fdaId, daId, query, params);
  }

  const stmt = await conn.prepare(query);
  paramValues = applyParams(paramValues, params);

  try {
    await stmt.bind(paramValues);
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

function applyParams(reqParams, params) {
  logger.debug({ reqParams, params }, '[DEBUG]: applyParams');
  if (!params) {
    return reqParams;
  }
  params.forEach((param) => {
    // Params: required
    if (!reqParams[param.name] && param.required) {
      throw new FDAError(
        400,
        'InvalidQueryParam',
        `Missing required param "${param.name}".`,
      );
    }
    // Params: default
    if (!reqParams[param.name] && param.default) {
      reqParams[param.name] = param.default;
    }

    const paramValue = reqParams[param.name];
    if (paramValue) {
      // Params: type
      if (param.type && !isTypeOf(paramValue, param.type)) {
        throw new FDAError(
          400,
          'InvalidQueryParam',
          `Param "${param.name}" not of valid type (${param.type}).`,
        );
      }
      // Params: range
      if (param.range && !isInRange(paramValue, param.range)) {
        throw new FDAError(
          400,
          'InvalidQueryParam',
          `Param "${param.name}" not in valid param range [${param.range}].`,
        );
      }
      // Params: enum
      if (param.enum && !isInEnum(paramValue, param.enum)) {
        throw new FDAError(
          400,
          'InvalidQueryParam',
          `Param "${param.name}" not in param enum [${param.enum}].`,
        );
      }
    }
  });
  return reqParams;
}

function isTypeOf(value, type) {
  const TYPE_COERCERS = {
    Numeric: (v) => (Number.isFinite(Number(v)) ? Number(v) : undefined),
    Boolean: (v) => v === 'true' || v === '1',
    String: (v) => String(v),
    Date: (v) => {
      // decode and replace for json coded values (e.g. + as %2B) and proper format
      const decoded = decodeURIComponent(v).replace(/([+-]\d{2})$/, '$1:00');
      return isNaN(new Date(decoded).getTime()) ? undefined : new Date(decoded);
    },
  };

  const coercer = TYPE_COERCERS[type];
  if (!coercer) {
    throw new FDAError(
      400,
      'InvalidQueryParam',
      `Invalid type value in params.`,
    );
  }

  return coercer(value);
}

function isInRange(value, range) {
  return value >= range[0] && value <= range[1];
}

function isInEnum(value, enumValues) {
  return enumValues.includes(value);
}

function fdaKey(service, fdaId) {
  return `${service}::${fdaId}`;
}
function getCachedQuery(service, fdaId, daId) {
  return cachedQueries.get(fdaKey(service, fdaId))?.get(daId);
}

export async function storeCachedQuery(
  conn,
  service,
  fdaId,
  daId,
  query,
  params,
) {
  const key = fdaKey(service, fdaId);
  let fda = cachedQueries.get(key);

  if (!fda) {
    fda = new Map();
    cachedQueries.set(key, fda);
  }

  // We create the pStatement in DuckDB to check compatibility with FDA baseQuery
  await conn.prepare(query);

  fda.set(daId, { query, params });
}

export function toParquet(conn, originPath, resultPath) {
  logger.debug({ originPath, resultPath }, '[DEBUG]: toParquet');
  return conn.run(
    `COPY ( SELECT * FROM read_csv_auto('s3://${originPath}')) 
    TO 's3://${resultPath}' (FORMAT PARQUET);`,
  );
}

export function buildDAQuery(service, fdaId, userQuery) {
  if (!userQuery || typeof userQuery !== 'string') {
    throw new FDAError(400, 'BadRequest', 'Invalid DA query');
  }

  if (/^\s*from\b/i.test(userQuery)) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'DA query must not include FROM clause at start. It is managed internally.',
    );
  }

  const trimmed = userQuery.trim();

  const parquetPath = `s3://${service}/${fdaId}.parquet`;

  return `FROM read_parquet('${parquetPath}') ${trimmed}`; // TO DISCUSS: implementation by adding FROM clause at the beginning of the query
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
