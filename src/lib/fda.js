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

import { getAgenda } from './jobs.js';
import {
  runPreparedStatement,
  runPreparedStatementStream,
  getDBConnection,
  releaseDBConnection,
  toParquet,
  checkParams,
  resolveDAParams,
  validateDAQuery,
  extractDate,
  PARTITION_TYPES,
  refreshIntervalPartitionCheck,
} from './utils/db.js';
import { uploadTable, runPgQuery, createPgCursorReader } from './utils/pg.js';
import {
  getS3Client,
  dropFile,
  moveObject,
  listObjects,
  dropFiles,
} from './utils/aws.js';
import {
  createFDAMongo,
  regenerateFDA,
  retrieveFDAs,
  retrieveFDA,
  storeDA,
  removeFDA,
  retrieveDAs,
  retrieveDA,
  updateDA,
  removeDA,
  updateFDAStatus,
} from './utils/mongo.js';
import {
  convertBigInt,
  getWindowDate,
  assertFreshQueriesEnabled,
  acquireFreshQuerySlot,
  getTimeColumnQuery,
} from './utils/utils.js';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

const FRESH_CURSOR_BATCH_SIZE = 250;
export const VALID_VISIBILITIES = ['public', 'private'];
const VALID_VISIBILITIES_SET = new Set(VALID_VISIBILITIES);

export async function getFDAs(service, visibility, servicePath) {
  const fdas = await retrieveFDAs(service);

  if (visibility === undefined && servicePath === undefined) {
    return fdas.map((fda) => toFDAApiResponse(fda, { includeId: true }));
  }

  const normalizedVisibility = normalizeVisibility(visibility);
  const normalizedServicePath = normalizeServicePath(servicePath);

  return fdas
    .filter(
      (fda) =>
        normalizeVisibility(fda.visibility) === normalizedVisibility &&
        normalizeServicePath(fda.servicePath) === normalizedServicePath,
    )
    .map((fda) => toFDAApiResponse(fda, { includeId: true }));
}

export async function getFDA(service, fdaId, visibility, servicePath) {
  if (visibility === undefined && servicePath === undefined) {
    const fda = await getStoredFDA(service, fdaId);
    return toFDAApiResponse(fda, { includeId: false });
  }

  const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);
  return toFDAApiResponse(fda, { includeId: false });
}

export async function executeQuery({
  service,
  visibility,
  servicePath,
  params,
  fresh = false,
}) {
  if (fresh) {
    return executeFreshQuery({ service, visibility, servicePath, params });
  }

  const { fdaId, daId, ...rest } = params;

  await ensureFDAReadyForQuery(service, fdaId, visibility, servicePath);

  const conn = await getDBConnection();

  try {
    return await runPreparedStatement(conn, service, fdaId, daId, rest);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function executeQueryStream({
  service,
  visibility,
  servicePath,
  params,
  req,
  res,
  fresh = false,
}) {
  if (fresh) {
    return executeFreshQueryStream({
      service,
      visibility,
      servicePath,
      params,
      req,
      res,
    });
  }

  const { fdaId, daId, ...rest } = params;

  await ensureFDAReadyForQuery(service, fdaId, visibility, servicePath);

  const conn = await getDBConnection();

  let stream;
  let close;

  try {
    const result = await runPreparedStatementStream(
      conn,
      service,
      fdaId,
      daId,
      rest,
    );

    stream = result.stream;
    close = result.close;
  } catch (err) {
    await releaseDBConnection(conn);
    throw err;
  }

  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) {
      return;
    }
    cleaned = true;

    try {
      await close();
    } finally {
      await releaseDBConnection(conn);
    }
  };

  req.on('close', () => {
    cleanup().catch(() => {});
  });

  res.setHeader('Content-Type', 'application/x-ndjson');

  try {
    const columnNames = stream.columnNames();
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const chunk = await stream.fetchChunk();
      if (chunk.rowCount === 0) {
        break;
      }

      const rows = chunk.getRows();

      const lines = [];

      for (const row of rows) {
        const rowObj = {};

        for (let i = 0; i < columnNames.length; i++) {
          rowObj[columnNames[i]] = row[i];
        }

        const safeObj = convertBigInt(rowObj);
        lines.push(JSON.stringify(safeObj));
      }

      const payload = lines.join('\n') + '\n';

      const ok = res.write(payload);
      if (!ok) {
        await new Promise((resolve) => res.once('drain', resolve));
      }
    }
  } finally {
    await cleanup();
  }

  return res.end();
}

async function executeFreshQuery({ service, visibility, servicePath, params }) {
  assertFreshQueriesEnabled(config.roles.syncQueries);

  const releaseFreshSlot = acquireFreshQuerySlot(
    config.freshQueries.maxConcurrent,
  );
  try {
    const { text, values } = await buildFreshQueryStatement(
      service,
      visibility,
      servicePath,
      params,
    );

    const rows = await runPgQuery(service, text, values);
    return convertBigInt(rows);
  } catch (e) {
    if (e instanceof FDAError) {
      throw e;
    }

    throw e;
  } finally {
    releaseFreshSlot();
  }
}

async function executeFreshQueryStream({
  service,
  visibility,
  servicePath,
  params,
  req,
  res,
}) {
  assertFreshQueriesEnabled(config.roles.syncQueries);

  const releaseFreshSlot = acquireFreshQuerySlot(
    config.freshQueries.maxConcurrent,
  );
  let cursorReader;

  try {
    const { text, values } = await buildFreshQueryStatement(
      service,
      visibility,
      servicePath,
      params,
    );

    cursorReader = await createPgCursorReader(
      service,
      text,
      values,
      FRESH_CURSOR_BATCH_SIZE,
    );

    req.on('close', () => {
      cursorReader?.close().catch(() => {});
    });

    res.setHeader('Content-Type', 'application/x-ndjson');

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const rows = await cursorReader.readNextChunk();
      if (rows.length === 0) {
        break;
      }

      for (const row of rows) {
        const safeObj = convertBigInt(row);
        const ok = res.write(JSON.stringify(safeObj) + '\n');
        if (!ok) {
          await new Promise((resolve) => res.once('drain', resolve));
        }
      }
    }
  } catch (e) {
    if (e instanceof FDAError) {
      throw e;
    }

    throw e;
  } finally {
    await cursorReader?.close();
    releaseFreshSlot();
  }

  return res.end();
}

async function buildFreshQueryStatement(
  service,
  visibility,
  servicePath,
  params,
) {
  const { fdaId, daId, ...rest } = params;

  const da = await retrieveDA(service, fdaId, daId);
  if (!da?.query) {
    throw new FDAError(
      404,
      'DaNotFound',
      `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
    );
  }

  const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);

  const validatedParams = resolveDAParams(rest || {}, da.params);
  const freshBaseQuery = buildFreshDAQuery(fda.query, da.query);

  return replaceNamedParamsWithPositional(freshBaseQuery, validatedParams);
}

function buildFreshDAQuery(fdaQuery, daQuery) {
  const cleanFdaQuery = removeTrailingSemicolon(fdaQuery?.trim() || '');
  const cleanDaQuery = removeTrailingSemicolon(daQuery?.trim() || '');

  if (!cleanDaQuery || /^from\b/i.test(cleanDaQuery)) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'DA query must not include FROM clause at start. It is managed internally.',
    );
  }

  if (!/^select\b/i.test(cleanDaQuery)) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'Fresh query mode requires DA query to start with SELECT.',
    );
  }

  const selectTail = cleanDaQuery.replace(/^select\s+/i, '');
  const clauseMatch = selectTail.match(
    /\b(where|group\s+by|having|order\s+by|limit|offset)\b/i,
  );

  const projection = clauseMatch
    ? selectTail.slice(0, clauseMatch.index).trim()
    : selectTail.trim();
  const clauses = clauseMatch ? selectTail.slice(clauseMatch.index).trim() : '';

  if (!projection) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'DA query must contain a SELECT projection.',
    );
  }

  if (/\bfrom\b/i.test(projection)) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'DA query must not include FROM clause. It is managed internally.',
    );
  }

  const trailing = clauses ? ` ${clauses}` : '';
  return `SELECT ${projection} FROM (${cleanFdaQuery}) AS fda_source${trailing}`;
}

function replaceNamedParamsWithPositional(query, params) {
  const indexes = new Map();
  const values = [];

  const text = query.replace(/\$([A-Za-z_][A-Za-z0-9_]*)/g, (_m, name) => {
    if (!Object.prototype.hasOwnProperty.call(params, name)) {
      throw new FDAError(
        400,
        'InvalidQueryParam',
        `Missing required param "${name}".`,
      );
    }

    if (!indexes.has(name)) {
      indexes.set(name, values.length + 1);
      values.push(params[name]);
    }

    return `$${indexes.get(name)}`;
  });

  return { text, values };
}

function removeTrailingSemicolon(query) {
  return query.replace(/;+\s*$/, '');
}

export async function createDA(
  service,
  fdaId,
  daId,
  description,
  userQuery,
  params,
  visibility,
  servicePath,
) {
  const conn = await getDBConnection();

  try {
    if (visibility !== undefined || servicePath !== undefined) {
      await getAccessibleFDA(service, fdaId, visibility, servicePath);
    }

    const existing = await retrieveDA(service, fdaId, daId);

    if (existing) {
      throw new FDAError(
        409,
        'DuplicatedKey',
        `DA ${daId} already exists in FDA ${fdaId}`,
      );
    }

    const normalizedParams = checkParams(params);
    await validateDAQuery(conn, service, fdaId, userQuery);
    await storeDA(
      service,
      fdaId,
      daId,
      description,
      userQuery,
      normalizedParams,
    );
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function fetchFDA(
  fdaId,
  query,
  service,
  visibility,
  servicePath,
  description,
  refreshPolicy,
  timeColumn,
  objStgConf,
) {
  validateScheduledOptions(refreshPolicy, objStgConf);
  const timeQuery =
    refreshPolicy?.type !== 'none' || objStgConf?.partition
      ? getTimeColumnQuery(query, timeColumn)
      : query;
  const normalizedVisibility = normalizeVisibility(visibility);
  const normalizedServicePath = normalizeServicePath(servicePath);

  await createFDAMongo(
    fdaId,
    timeQuery,
    service,
    normalizedVisibility,
    normalizedServicePath,
    description,
    refreshPolicy,
    timeColumn,
    objStgConf,
  );

  try {
    await createOneRowParquetSync(service, fdaId, timeQuery);
  } catch (err) {
    await rollbackFDAProvisioning(service, fdaId);
    throw err;
  }

  const agenda = getAgenda();

  // Execute first fetch immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: timeQuery,
    service,
    timeColumn,
    objStgConf,
  });

  // Schedule refreshes according to policy
  if (refreshPolicy?.type === 'interval' || refreshPolicy?.type === 'cron') {
    const { refreshInterval, windowSize } = refreshPolicy.params || {};

    // unique is not really needed since we check existence before, but it adds an extra layer of safety in case of duplicate calls
    await agenda.every(
      refreshInterval,
      'refresh-fda',
      { fdaId, query: timeQuery, service, timeColumn, objStgConf },
      {
        skipImmediate: true,
        unique: {
          name: 'refresh-fda',
          'data.fdaId': fdaId,
        },
      },
    );

    if (windowSize) {
      await agenda.every(
        refreshInterval,
        'clean-partition',
        {
          fdaId,
          service,
          windowSize,
          objStgConf,
        },
        {
          skipImmediate: true,
          unique: {
            name: 'refresh-fda',
            'data.fdaId': fdaId,
          },
        },
      );
    }
  }

  if (refreshPolicy?.type === 'window') {
    const { refreshInterval, windowSize } = refreshPolicy.params || {};

    // partitionFlag lets us know we are refreshing already existing partitioned files for performance purposes
    await agenda.every(
      refreshInterval,
      'refresh-fda',
      {
        fdaId,
        query: timeQuery,
        service,
        timeColumn,
        refreshPolicy,
        objStgConf,
        partitionFlag: true,
      },
      {
        skipImmediate: true,
        unique: {
          name: 'refresh-fda',
          'data.fdaId': fdaId,
        },
      },
    );

    if (windowSize) {
      await agenda.every(
        refreshInterval,
        'clean-partition',
        {
          fdaId,
          service,
          windowSize,
          objStgConf,
        },
        {
          skipImmediate: true,
          unique: {
            name: 'refresh-fda',
            'data.fdaId': fdaId,
          },
        },
      );
    }
  }
}

function validateScheduledOptions(refreshPolicy, objStgConf) {
  if (!refreshPolicy || refreshPolicy.type === 'none') {
    return;
  }
  const { refreshInterval, fetchSize } = refreshPolicy.params || {};
  if (!refreshInterval) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Missing required refresh policy parameter: refreshInterval.`,
    );
  }

  if (
    objStgConf?.partition &&
    !PARTITION_TYPES.includes(objStgConf.partition)
  ) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid partition type "${objStgConf.partition}".`,
    );
  }

  // RefreshInterval must be smaller or equal than partition size
  if (!refreshIntervalPartitionCheck(refreshInterval, objStgConf?.partition)) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Refresh interval "${refreshInterval}" must be smaller or equal than partition size "${objStgConf?.partition}".`,
    );
  }

  // fetched data size must be equal than partition size (if both presents).
  if (objStgConf?.partition && fetchSize !== objStgConf?.partition) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Fetch size "${fetchSize}" must be equal to partition size "${objStgConf?.partition}".`,
    );
  }
}

export async function updateFDA(service, fdaId, visibility, servicePath) {
  if (arguments.length >= 4) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  const previous = await regenerateFDA(service, fdaId);

  const agenda = getAgenda();

  // Execute refresh immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: previous.query,
    service,
    timeColumn: previous.timeColumn,
    objStgConf: previous.objStgConf,
    partitionFlag: true,
  });
}

export async function processFDAAsync(
  fdaId,
  query,
  service,
  timeColumn,
  refreshPolicy,
  objStgConf,
  partitionFlag,
) {
  try {
    await updateFDAStatus(service, fdaId, 'fetching', 10);

    let finalQuery = query;
    if (refreshPolicy?.type === 'window') {
      const { params = {} } = refreshPolicy;
      const prevWindowStartDate = getPreviousWindowStartDate(params.fetchSize);
      finalQuery = getUpdateWindowQuery(query, timeColumn, prevWindowStartDate);
    }

    await uploadTableToObjStg(
      service,
      service,
      finalQuery,
      service,
      fdaId,
      timeColumn,
      objStgConf,
      partitionFlag,
    );

    await updateFDAStatus(service, fdaId, 'completed', 100);
  } catch (err) {
    await updateFDAStatus(service, fdaId, 'failed', 0, err.message);
    throw err;
  }
}

function getPreviousWindowStartDate(fetchSize) {
  const now = new Date();

  switch (fetchSize) {
    case 'day': {
      const d = new Date(now);
      d.setUTCDate(d.getUTCDate() - 1);
      d.setUTCHours(0, 0, 0, 0);
      return d.toISOString();
    }

    case 'week': {
      const d = new Date(now);
      d.setUTCDate(d.getUTCDate() - 7);
      d.setUTCHours(0, 0, 0, 0);
      return d.toISOString();
    }

    case 'month': {
      const d = new Date(now);
      d.setUTCMonth(d.getUTCMonth() - 1);
      d.setUTCHours(0, 0, 0, 0);
      return d.toISOString();
    }

    case 'year': {
      const d = new Date(now);
      d.setUTCFullYear(d.getUTCFullYear() - 1);
      d.setUTCHours(0, 0, 0, 0);
      return d.toISOString();
    }

    default:
      throw new FDAError(400, 'InvalidParam', `Missing param fetchSize.`);
  }
}

function getUpdateWindowQuery(query, timeColumn, latestFetchStartDate) {
  return `SELECT * FROM (${query}) q WHERE ${timeColumn} >= TIMESTAMP '${latestFetchStartDate}' AND ${timeColumn} < NOW()`;
}

export async function deleteFDA(service, fdaId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  const { _id } = (await retrieveFDA(service, fdaId)) ?? {};

  if (!service || !_id) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} of the service ${service} not found.`,
    );
  }
  const s3Client = await getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  // This way we remove FDAs independently of if theyre partitioned or not
  const objPaths = await listObjects(s3Client, service, fdaId);
  await dropFiles(s3Client, service, objPaths);

  await removeFDA(service, fdaId);

  const agenda = getAgenda();
  await agenda.cancel({
    name: 'refresh-fda',
    'data.fdaId': fdaId,
  });
  await agenda.cancel({
    name: 'clean-partition',
    'data.fdaId': fdaId,
  });
}

export async function getDAs(service, fdaId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  return retrieveDAs(service, fdaId);
}

export async function getDA(service, fdaId, daId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  const da = await retrieveDA(service, fdaId, daId);
  if (da) {
    da.id = daId;
  } else {
    throw new FDAError(
      404,
      'DaNotFound',
      `DA ${daId} not found in FDA ${fdaId} and service ${service}.`,
    );
  }

  return da;
}

export async function putDA(
  service,
  fdaId,
  daId,
  description,
  userQuery,
  params,
  visibility,
  servicePath,
) {
  const conn = await getDBConnection();

  try {
    if (visibility !== undefined || servicePath !== undefined) {
      await getAccessibleFDA(service, fdaId, visibility, servicePath);
    }

    const normalizedParams = checkParams(params);
    await validateDAQuery(conn, service, fdaId, userQuery);
    await updateDA(
      service,
      fdaId,
      daId,
      description,
      userQuery,
      normalizedParams,
    );
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function deleteDA(service, fdaId, daId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  await removeDA(service, fdaId, daId);
}

export async function cleanPartition(service, fdaId, windowSize, objStgConf) {
  if (!objStgConf?.partition) {
    // DEBATE: With no partitioned folders doesn't make much sense to clean cause we'd had a FDA with no file
    throw new FDAError(
      400,
      'CleaningError',
      `Removing a non partitioned FDA ${fdaId}.`,
    );
  }

  const cutoff = getWindowDate(windowSize);
  if (!cutoff) {
    throw new FDAError(
      400,
      'CleaningError',
      `Incorrect window size in refresh policy.`,
    );
  }

  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );

  const objPaths = await listObjects(s3Client, service, fdaId);

  const partitionsToRemove = [];
  for (const path of objPaths) {
    const partitionDate = extractDate(path);

    if (partitionDate < cutoff) {
      partitionsToRemove.push(path);
    }
  }
  await dropFiles(s3Client, service, partitionsToRemove);
}

async function uploadTableToObjStg(
  service,
  database,
  query,
  bucket,
  path,
  timeColumn,
  objStgConf,
  partitionFlag,
) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  await updateFDAStatus(service, path, 'fetching', 20);
  await uploadTable(s3Client, bucket, database, query, path);

  const conn = await getDBConnection();
  try {
    await updateFDAStatus(service, path, 'transforming', 60);

    // DuckDB cant overwrite files in Minio, so for partitioned files we upload them in a tmp file and the move them
    // We only do this for files that already exist (partitionFlag=true) so upload performance on partitions doesnt get affected
    const parquetPath = partitionFlag
      ? getPath(bucket, 'tmp/' + path, '.parquet')
      : getPath(bucket, path, '.parquet');

    await toParquet(
      conn,
      getPath(bucket, path, '.csv'),
      parquetPath,
      timeColumn,
      objStgConf?.partition,
      objStgConf?.compression,
    );

    if (partitionFlag) {
      const objectsList = await listObjects(
        s3Client,
        bucket,
        `tmp/${path}.parquet`,
      );
      for (const tempPartition of objectsList) {
        await moveObject(
          s3Client,
          bucket,
          `${bucket}/${tempPartition}`,
          tempPartition.replace('tmp/', ''),
        );
        await dropFile(s3Client, bucket, tempPartition);
      }
    }

    await updateFDAStatus(service, path, 'uploading', 80);
    // DuckDb doesn't replace one row parquet snippet with partitioned file, so we remove it by hand
    if (objStgConf?.partition) {
      await dropFile(s3Client, bucket, `${path}.parquet`);
    }
    await dropFile(s3Client, bucket, `${path}.csv`);
  } catch (e) {
    throw new FDAError(500, 'UploadError', e.message);
  } finally {
    await releaseDBConnection(conn);
  }
}

async function ensureFDAReadyForQuery(service, fdaId, visibility, servicePath) {
  const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);

  // Queries are blocked only before the first successful fetch.
  if (!fda.lastFetch) {
    throw new FDAError(
      409,
      'FDAUnavailable',
      `FDA ${fdaId} is not queryable yet because the first fetch has not completed`,
    );
  }
}

async function getStoredFDA(service, fdaId) {
  const fda = await retrieveFDA(service, fdaId);

  if (!fda) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} not found in service ${service}`,
    );
  }

  return fda;
}

async function getAccessibleFDA(service, fdaId, visibility, servicePath) {
  const normalizedVisibility = normalizeVisibility(visibility);
  const normalizedServicePath = normalizeServicePath(servicePath);
  const fda = await getStoredFDA(service, fdaId);

  if (normalizeVisibility(fda.visibility) !== normalizedVisibility) {
    throw new FDAError(
      403,
      'VisibilityMismatch',
      `FDA ${fdaId} does not belong to ${normalizedVisibility}`,
    );
  }

  if (normalizeServicePath(fda.servicePath) !== normalizedServicePath) {
    throw new FDAError(
      403,
      'ServicePathMismatch',
      `FDA ${fdaId} does not belong to servicePath ${normalizedServicePath}`,
    );
  }

  return fda;
}

function normalizeVisibility(visibility) {
  if (!VALID_VISIBILITIES_SET.has(visibility)) {
    throw new FDAError(
      400,
      'InvalidVisibility',
      'Visibility must be public or private',
    );
  }

  return visibility;
}

function normalizeServicePath(servicePath) {
  if (!servicePath || typeof servicePath !== 'string') {
    throw new FDAError(
      400,
      'InvalidServicePath',
      'Fiware-ServicePath header is required',
    );
  }

  const normalizedServicePath = servicePath.trim();

  if (!/^\/(?:[^/\s]+(?:\/[^/\s]+)*)?$/.test(normalizedServicePath)) {
    throw new FDAError(
      400,
      'InvalidServicePath',
      'Fiware-ServicePath must be a valid absolute path (e.g. / or /servicepath)',
    );
  }

  return normalizedServicePath;
}

function toFDAApiResponse(fda, { includeId }) {
  if (!fda) {
    return fda;
  }

  const response = { ...fda };
  const fdaId = response.fdaId;

  delete response._id;
  delete response.fdaId;
  delete response.service;
  delete response.visibility;
  delete response.servicePath;

  if (!includeId) {
    return response;
  }

  return {
    id: fdaId,
    ...response,
  };
}

async function createOneRowParquetSync(service, fdaId, query) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );

  const oneRowQuery = buildOneRowQuery(query);
  await uploadTable(s3Client, service, service, oneRowQuery, fdaId);

  const conn = await getDBConnection();
  try {
    const parquetPath = getPath(service, fdaId, '.parquet');
    await toParquet(conn, getPath(service, fdaId, '.csv'), parquetPath);
    await dropFile(s3Client, service, `${fdaId}.csv`);
  } finally {
    await releaseDBConnection(conn);
  }
}

function buildOneRowQuery(query) {
  const normalizedQuery = query.trim().replace(/;+\s*$/, '');
  return `SELECT * FROM (${normalizedQuery}) AS fda_one_row LIMIT 1`;
}

async function rollbackFDAProvisioning(service, fdaId) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );

  await Promise.allSettled([
    dropFile(s3Client, service, `${fdaId}.csv`),
    dropFile(s3Client, service, `${fdaId}.parquet`),
    removeFDA(service, fdaId),
  ]);
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};
