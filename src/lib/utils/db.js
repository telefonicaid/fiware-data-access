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

import { retrieveDA, retrieveFDA } from './mongo.js';
import { FDAError } from '../fdaError.js';
import { getBasicLogger } from './logger.js';
import { config } from '../fdaConfig.js';
import { getFDAStoragePath } from './fdaScope.js';

let instance = null;

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
  servicePath,
  streaming = false,
) {
  const method = streaming
    ? 'runPreparedStatementStream'
    : 'runPreparedStatement';
  logger.debug(
    streaming
      ? { service, fdaId, daId, paramValues }
      : { service, fdaId, daId, paramValues: JSON.stringify(paramValues) },
    `[DEBUG]: ${method}`,
  );

  const da = await retrieveDA(service, fdaId, daId, servicePath);
  if (!da?.query) {
    throw new FDAError(
      404,
      'DaNotFound',
      `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
    );
  }

  const { objStgConf, servicePath: storedServicePath } = await retrieveFDA(
    service,
    fdaId,
    servicePath,
  );
  const query = buildDAQuery(
    service,
    fdaId,
    da.query,
    objStgConf?.partition,
    storedServicePath ?? servicePath,
  );

  const stmt = await conn.prepare(query);

  try {
    const resolvedParams = applyParams(paramValues || {}, da.params);
    const boundParams = normalizeParamsForDuckDB(resolvedParams);
    await stmt.bind(boundParams);

    if (streaming) {
      const stream = await stmt.stream();
      const close = async () => {
        if (typeof stmt.close === 'function') {
          await stmt.close();
        }
      };
      return { stream, close };
    }

    const result = await stmt.run();
    return result.getRowObjectsJson();
  } catch (e) {
    if (streaming && typeof stmt.close === 'function') {
      await stmt.close();
    }

    if (e instanceof FDAError) {
      throw e;
    }

    const action = streaming ? 'streaming' : 'running';
    throw new FDAError(
      500,
      'DuckDBServerError',
      `Error ${action} the prepared statement: ${e}`,
    );
  } finally {
    if (!streaming && typeof stmt.close === 'function') {
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
  servicePath,
) {
  return await runPreparedStatement(
    conn,
    service,
    fdaId,
    daId,
    paramValues,
    servicePath,
    true,
  );
}

export function checkParams(params) {
  if (!params) {
    return params;
  }

  return params.map((param) => {
    if (!param.type) {
      throw new FDAError(
        400,
        'InvalidParam',
        `Type is a mandatory key in every param.`,
      );
    }

    if (!Object.keys(TYPE_COERCERS).includes(param.type)) {
      throw new FDAError(400, 'InvalidParam', `Invalid value in type key.`);
    }

    if (param.range) {
      const range = param.range;

      if (typeof range[0] !== 'number' || typeof range[1] !== 'number') {
        throw new FDAError(
          400,
          'InvalidParam',
          `Both values of range param should be of type number".`,
        );
      }
      if (range[0] > range[1]) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Fisrt number should be smaller than second number.`,
        );
      }
      if (range.length > 2) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Range cant have more than two values.`,
        );
      }
    }

    if (param.enum) {
      param.enum.forEach((v) => {
        if (typeof v !== 'string' && typeof v !== 'number') {
          throw new FDAError(
            400,
            'InvalidParam',
            `Values of enum param should be strings or numbers".`,
          );
        }
      });
    }

    const normalizedParam = { ...param };

    if (Object.prototype.hasOwnProperty.call(param, 'default')) {
      const coercedDefault = isTypeOf(param.default, param.type);

      if (coercedDefault === undefined) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Default value for param "${param.name}" not of valid type (${param.type}).`,
        );
      }

      if (param.range && !isInRange(coercedDefault, param.range)) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Default value for param "${param.name}" not in valid param range [${param.range}].`,
        );
      }

      if (param.enum && !isInEnum(coercedDefault, param.enum)) {
        throw new FDAError(
          400,
          'InvalidParam',
          `Default value for param "${param.name}" not in param enum [${param.enum}].`,
        );
      }

      normalizedParam.default = normalizeParamDefaultForStorage(coercedDefault);
    }

    return normalizedParam;
  });
}

function normalizeParamDefaultForStorage(value) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return value;
}

function applyParams(reqParams, params) {
  logger.debug({ reqParams, params }, '[DEBUG]: applyParams start');

  if (!Array.isArray(params) || params.length === 0) {
    return {};
  }

  const validated = {};

  for (const param of params) {
    let value = reqParams[param.name];

    // Required
    if ((value === undefined || value === null) && param.required) {
      throw new FDAError(
        400,
        'InvalidQueryParam',
        `Missing required param "${param.name}".`,
      );
    }

    // Default
    if (
      (value === undefined || value === null) &&
      param.default !== undefined
    ) {
      value = param.default;
    }

    if (value !== undefined) {
      // Type coercion
      if (param.type) {
        const coerced = isTypeOf(value, param.type);

        if (coerced === undefined) {
          throw new FDAError(
            400,
            'InvalidQueryParam',
            `Param "${param.name}" not of valid type (${param.type}).`,
          );
        }

        value = coerced;
      }

      // Range
      if (param.range && !isInRange(value, param.range)) {
        throw new FDAError(
          400,
          'InvalidQueryParam',
          `Param "${param.name}" not in valid param range [${param.range}].`,
        );
      }

      // Enum
      if (param.enum && !isInEnum(value, param.enum)) {
        throw new FDAError(
          400,
          'InvalidQueryParam',
          `Param "${param.name}" not in param enum [${param.enum}].`,
        );
      }

      validated[param.name] = value;
    }
  }

  return validated;
}

function normalizeParamsForDuckDB(params) {
  const normalized = {};

  for (const [key, value] of Object.entries(params)) {
    let normalizedValue = value;

    if (normalizedValue instanceof Date) {
      normalizedValue = normalizedValue.toISOString();
    }

    if (typeof normalizedValue === 'boolean') {
      normalizedValue = normalizedValue ? 1 : 0;
    }

    normalized[key] = normalizedValue;
  }

  return normalized;
}

export function resolveDAParams(reqParams, params) {
  return applyParams(reqParams, params);
}

const TYPE_COERCERS = {
  Number: (v) => (Number.isFinite(Number(v)) ? Number(v) : undefined),
  Boolean: (v) => {
    if (v === true || v === false) {
      return v;
    }
    if (v === 'true' || v === '1') {
      return true;
    }
    if (v === 'false' || v === '0') {
      return false;
    }
    return undefined;
  },
  Text: (v) => (v == null ? undefined : String(v)),
  DateTime: (v) => {
    if (v instanceof Date) {
      return Number.isNaN(v.getTime()) ? undefined : v;
    }

    if (typeof v !== 'string') {
      return undefined;
    }
    let decoded;
    try {
      decoded = decodeURIComponent(v);
    } catch {
      return undefined;
    }

    // strict ISO 8601 (UTC or offset)
    const ISO_8601 =
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

    if (!ISO_8601.test(decoded)) {
      return undefined;
    }

    const date = new Date(decoded);
    return Number.isNaN(date.getTime()) ? undefined : date;
  },
};

function isTypeOf(value, type) {
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

export function toParquet(
  conn,
  originPath,
  resultPath,
  timeColumn,
  partitionType,
  compression,
) {
  logger.debug({ originPath, resultPath }, '[DEBUG]: toParquet');

  const { cols, partitionBy } = getPartitionConf(partitionType, timeColumn);
  const compressionString = compression ? `, COMPRESSION ZSTD` : '';

  return conn.run(
    `COPY ( SELECT ${cols}
                FROM read_csv_auto('s3://${originPath}')) 
      TO 's3://${resultPath}' (FORMAT PARQUET ${partitionBy} ${compressionString});`,
  );
}

function getPartitionConf(partitionType = 'none', timeColumn) {
  if (!timeColumn && partitionType !== 'none') {
    throw new FDAError(400, 'PartitionError', `Missing timeColumn value.`);
  }
  const PARTITIONS = {
    day: {
      columns: `
      year(${timeColumn}) as year,
      month(${timeColumn}) as month,
      day(${timeColumn}) as day
    `,
      partitionBy: 'year, month, day',
    },
    week: {
      columns: `
      year(${timeColumn}) as year,
      strftime(${timeColumn}, '%Y-%W') as week
      `,
      partitionBy: 'year, week',
    },
    month: {
      columns: `
      year(${timeColumn}) as year,
      month(${timeColumn}) as month
    `,
      partitionBy: 'year, month',
    },
    year: {
      columns: `
      year(${timeColumn}) as year
    `,
      partitionBy: 'year',
    },
    none: {
      columns: '',
      partitionBy: '',
    },
  };

  const partitionConf = PARTITIONS[partitionType];
  if (!partitionConf) {
    throw new FDAError(
      400,
      'PartitionError',
      `Incorrect partition type: ${partitionType}.`,
    );
  }

  const cols = partitionConf.columns ? `*, ${partitionConf.columns}` : '*';
  const partitionBy = partitionConf.partitionBy
    ? `, PARTITION_BY (${partitionConf.partitionBy})`
    : '';

  return { cols, partitionBy };
}

export function buildDAQuery(
  service,
  fdaId,
  userQuery,
  partition,
  servicePath,
) {
  logger.debug(
    { service, fdaId, userQuery, partition },
    '[DEBUG]: buildDAQuery',
  );
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

  const objectKey = getFDAStoragePath(fdaId, servicePath);
  const parquetPath = `s3://${service}/${objectKey}.parquet`;

  if (partition) {
    return `FROM read_parquet('${parquetPath}/**/*.parquet') ${trimmed}`;
  } else {
    return `FROM read_parquet('${parquetPath}') ${trimmed}`;
  }
}

export function extractDate(path) {
  const year = path.match(/year=(\d{4})/)?.[1];
  const month = path.match(/month=(\d{1,2})/)?.[1];
  const day = path.match(/day=(\d{1,2})/)?.[1];
  const weekMatch = path.match(/week=(\d{4})-(\d{1,2})/);

  const week = weekMatch?.[2]; // second group = week number

  if (year && month && day) {
    return new Date(Date.UTC(year, month - 1, day));
  }

  if (year && month) {
    return new Date(Date.UTC(year, month - 1, 1));
  }

  if (year && week) {
    const d = new Date(Date.UTC(year, 0, 1 + (week - 1) * 7));
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);
    return d;
  }

  return null;
}

export async function validateDAQuery(
  conn,
  service,
  fdaId,
  userQuery,
  servicePath,
) {
  const query = buildDAQuery(service, fdaId, userQuery, undefined, servicePath);

  let stmt;
  try {
    stmt = await conn.prepare(query);
  } catch (e) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      `DA query is not compatible with FDA ${fdaId}: ${e.message || e}`,
    );
  } finally {
    if (stmt && typeof stmt.close === 'function') {
      await stmt.close();
    }
  }
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
