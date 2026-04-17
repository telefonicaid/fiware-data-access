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
  normalizeForSerialization,
  getWindowDate,
  assertFreshQueriesEnabled,
  acquireFreshQuerySlot,
  getTimeColumnQuery,
} from './utils/utils.js';
import {
  buildFDAJobFilter,
  getBucketNameFromService,
  getFDAStoragePath,
  normalizeScopedServicePath,
} from './utils/fdaScope.js';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

const FRESH_CURSOR_BATCH_SIZE = 250;
export const VALID_VISIBILITIES = ['public', 'private'];
const VALID_VISIBILITIES_SET = new Set(VALID_VISIBILITIES);
const CSV_CONTENT_TYPE = 'text/csv; charset=utf-8';

function stringifyCsvValue(value) {
  const normalizedValue = normalizeForSerialization(value);

  if (normalizedValue === null || normalizedValue === undefined) {
    return '';
  }

  if (typeof normalizedValue === 'object') {
    return JSON.stringify(normalizedValue);
  }

  return String(normalizedValue);
}

function escapeCsvValue(value) {
  const strValue = stringifyCsvValue(value);

  if (
    strValue.includes(',') ||
    strValue.includes('"') ||
    strValue.includes('\n') ||
    strValue.includes('\r')
  ) {
    return '"' + strValue.replace(/"/g, '""') + '"';
  }

  return strValue;
}

async function writeCsvLine(res, line) {
  const ok = res.write(line);
  if (!ok) {
    await new Promise((resolve) => res.once('drain', resolve));
  }
}

async function writeNdjsonLine(res, row) {
  const safeObj = normalizeForSerialization(row);
  const ok = res.write(JSON.stringify(safeObj) + '\n');
  if (!ok) {
    await new Promise((resolve) => res.once('drain', resolve));
  }
}

async function writeCsvHeader(res, columnNames) {
  if (columnNames.length === 0) {
    return;
  }

  await writeCsvLine(
    res,
    columnNames.map((columnName) => escapeCsvValue(columnName)).join(',') +
      '\n',
  );
}

function toRowObject(row, columnNames) {
  const rowObj = {};

  for (let i = 0; i < columnNames.length; i++) {
    rowObj[columnNames[i]] = row[i];
  }

  return rowObj;
}

export async function getFDAs(service, visibility, servicePath) {
  const fdas = await retrieveFDAs(service);
  const normalizedServicePath = normalizeServicePath(servicePath);

  if (visibility === undefined) {
    return fdas
      .filter(
        (fda) =>
          normalizeServicePath(fda.servicePath) === normalizedServicePath,
      )
      .map((fda) => toFDAApiResponse(fda, { includeId: true }));
  }

  const normalizedVisibility = normalizeVisibility(visibility);

  return fdas
    .filter(
      (fda) =>
        normalizeVisibility(fda.visibility) === normalizedVisibility &&
        normalizeServicePath(fda.servicePath) === normalizedServicePath,
    )
    .map((fda) => toFDAApiResponse(fda, { includeId: true }));
}

export async function getFDA(service, fdaId, visibility, servicePath) {
  const normalizedServicePath = normalizeServicePath(servicePath);

  if (visibility === undefined) {
    const fda = await getStoredFDA(service, fdaId, normalizedServicePath);
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
    const rows = await runPreparedStatement(
      conn,
      service,
      fdaId,
      daId,
      rest,
      servicePath,
    );

    return normalizeForSerialization(rows);
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
  format = 'ndjson',
}) {
  const source = fresh
    ? await createFreshRowSource({
        service,
        visibility,
        servicePath,
        params,
        req,
      })
    : await createCachedRowSource({
        service,
        visibility,
        servicePath,
        params,
        req,
      });

  if (format === 'ndjson') {
    res.setHeader('Content-Type', 'application/x-ndjson');
  } else {
    res.setHeader('Content-Type', CSV_CONTENT_TYPE);
    res.setHeader('Content-Disposition', 'attachment; filename="results.csv"');
  }

  let csvColumns;

  try {
    for (
      let rows = await source.readNextRows();
      rows.length > 0;
      rows = await source.readNextRows()
    ) {
      if (format === 'ndjson') {
        for (const row of rows) {
          const rowObj = source.columnNames
            ? toRowObject(row, source.columnNames)
            : row;
          await writeNdjsonLine(res, rowObj);
        }
        continue;
      }

      if (!csvColumns) {
        csvColumns = source.columnNames ?? Object.keys(rows[0] ?? {});
        await writeCsvHeader(res, csvColumns);
      }

      for (const row of rows) {
        const csvLine = source.columnNames
          ? row.map((cell) => escapeCsvValue(cell)).join(',') + '\n'
          : csvColumns.map((column) => escapeCsvValue(row[column])).join(',') +
            '\n';
        await writeCsvLine(res, csvLine);
      }
    }
  } finally {
    await source.close();
  }

  return res.end();
}

async function createCachedRowSource({
  service,
  visibility,
  servicePath,
  params,
  req,
}) {
  const { fdaId, daId, ...rest } = params;

  await ensureFDAReadyForQuery(service, fdaId, visibility, servicePath);

  const conn = await getDBConnection();

  let stream;
  let closeStream;

  try {
    const result = await runPreparedStatementStream(
      conn,
      service,
      fdaId,
      daId,
      rest,
      servicePath,
    );

    stream = result.stream;
    closeStream = result.close;
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
      await closeStream();
    } finally {
      await releaseDBConnection(conn);
    }
  };

  req.on('close', () => {
    cleanup().catch(() => {});
  });

  return {
    columnNames: stream.columnNames(),
    async readNextRows() {
      const chunk = await stream.fetchChunk();
      return chunk.rowCount > 0 ? chunk.getRows() : [];
    },
    close: cleanup,
  };
}

async function createFreshRowSource({
  service,
  visibility,
  servicePath,
  params,
  req,
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

    return {
      columnNames: null,
      readNextRows: () => cursorReader.readNextChunk(),
      close: async () => {
        await cursorReader?.close();
        releaseFreshSlot();
      },
    };
  } catch (e) {
    releaseFreshSlot();
    throw e;
  }
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
    return normalizeForSerialization(rows);
  } catch (e) {
    if (e instanceof FDAError) {
      throw e;
    }

    throw e;
  } finally {
    releaseFreshSlot();
  }
}

async function buildFreshQueryStatement(
  service,
  visibility,
  servicePath,
  params,
) {
  const { fdaId, daId, ...rest } = params;

  const da = await retrieveDA(service, fdaId, daId, servicePath);
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
      /* c8 ignore next 5 */
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

    const existing = await retrieveDA(service, fdaId, daId, servicePath);

    if (existing) {
      throw new FDAError(
        409,
        'DuplicatedKey',
        `DA ${daId} already exists in FDA ${fdaId}`,
      );
    }

    const normalizedParams = checkParams(params);
    await validateDAQuery(conn, service, fdaId, userQuery, servicePath);
    await storeDA(
      service,
      fdaId,
      servicePath,
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
  defaultDataAccessEnabled,
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
    await createOneRowParquetSync(
      service,
      fdaId,
      timeQuery,
      normalizedServicePath,
    );
  } catch (err) {
    await rollbackFDAProvisioning(service, fdaId, normalizedServicePath);
    throw err;
  }

  if (defaultDataAccessEnabled) {
    try {
      const defaultDA = await buildDefaultDataAccessDefinition(
        service,
        fdaId,
        normalizedServicePath,
        timeColumn,
      );

      await createDA(
        service,
        fdaId,
        'defaultDataAccess',
        'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
        defaultDA.query,
        defaultDA.params,
        normalizedVisibility,
        normalizedServicePath,
      );
    } catch (err) {
      // If default DA creation fails, rollback the entire FDA
      await rollbackFDAProvisioning(service, fdaId, normalizedServicePath);
      throw new FDAError(
        500,
        'DefaultDataAccessCreationError',
        `Failed to create default Data Access for FDA ${fdaId}: ${err.message}`,
      );
    }
  }

  const agenda = getAgenda();

  // Execute first fetch immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: timeQuery,
    service,
    servicePath: normalizedServicePath,
    timeColumn,
    refreshPolicy,
    objStgConf,
  });

  // Schedule refreshes according to policy
  if (refreshPolicy?.type === 'interval') {
    const { refreshInterval, windowSize } = refreshPolicy.params || {};

    // unique is not really needed since we check existence before, but it adds an extra layer of safety in case of duplicate calls
    await agenda.every(
      refreshInterval,
      'refresh-fda',
      {
        fdaId,
        query: timeQuery,
        service,
        servicePath: normalizedServicePath,
        timeColumn,
        refreshPolicy,
        objStgConf,
      },
      {
        skipImmediate: true,
        unique: buildFDAJobFilter(
          'refresh-fda',
          service,
          fdaId,
          normalizedServicePath,
        ),
      },
    );

    if (windowSize) {
      await agenda.every(
        refreshInterval,
        'clean-partition',
        {
          fdaId,
          service,
          servicePath: normalizedServicePath,
          windowSize,
          objStgConf,
        },
        {
          skipImmediate: true,
          unique: buildFDAJobFilter(
            'clean-partition',
            service,
            fdaId,
            normalizedServicePath,
          ),
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
        servicePath: normalizedServicePath,
        timeColumn,
        refreshPolicy,
        objStgConf,
        partitionFlag: true,
      },
      {
        skipImmediate: true,
        unique: buildFDAJobFilter(
          'refresh-fda',
          service,
          fdaId,
          normalizedServicePath,
        ),
      },
    );

    if (windowSize) {
      await agenda.every(
        refreshInterval,
        'clean-partition',
        {
          fdaId,
          service,
          servicePath: normalizedServicePath,
          windowSize,
          objStgConf,
        },
        {
          skipImmediate: true,
          unique: buildFDAJobFilter(
            'clean-partition',
            service,
            fdaId,
            normalizedServicePath,
          ),
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
  const normalizedServicePath = normalizeServicePath(servicePath);

  if (visibility !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  const previous = await regenerateFDA(service, fdaId, normalizedServicePath);

  const agenda = getAgenda();

  // Execute refresh immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: previous.query,
    service,
    servicePath: previous.servicePath ?? normalizedServicePath,
    timeColumn: previous.timeColumn,
    refreshPolicy: previous.refreshPolicy,
    objStgConf: previous.objStgConf,
    partitionFlag: true,
  });

  if (previous.refreshPolicy?.params?.windowSize) {
    await agenda.now('clean-partition', {
      fdaId,
      service,
      windowSize: previous.refreshPolicy.params.windowSize,
      objStgConf: previous.objStgConf,
    });
  }
}

export async function processFDAAsync(
  fdaId,
  query,
  service,
  servicePath,
  timeColumn,
  refreshPolicy,
  objStgConf,
  partitionFlag,
) {
  const storagePath = getFDAStoragePath(fdaId, servicePath);
  const bucketName = getBucketNameFromService(service);

  try {
    await updateFDAStatus(service, fdaId, servicePath, 'fetching', 10);

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
      bucketName,
      storagePath,
      fdaId,
      servicePath,
      timeColumn,
      objStgConf,
      partitionFlag,
    );

    await updateFDAStatus(service, fdaId, servicePath, 'completed', 100);
  } catch (err) {
    await updateFDAStatus(
      service,
      fdaId,
      servicePath,
      'failed',
      0,
      err.message,
    );
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
  let targetServicePath = normalizeServicePath(servicePath);

  if (visibility !== undefined || servicePath !== undefined) {
    const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);
    targetServicePath = fda.servicePath;
  }

  const { _id } = (await retrieveFDA(service, fdaId, targetServicePath)) ?? {};

  if (!service || !_id) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} of the service ${service} not found.`,
    );
  }
  const bucketName = getBucketNameFromService(service);
  const s3Client = await getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  // This way we remove FDAs independently of if theyre partitioned or not
  const objPaths = await listObjects(
    s3Client,
    bucketName,
    getFDAStoragePath(fdaId, targetServicePath),
  );
  await dropFiles(s3Client, bucketName, objPaths);

  await removeFDA(service, fdaId, targetServicePath);

  const agenda = getAgenda();
  await agenda.cancel(
    buildFDAJobFilter('refresh-fda', service, fdaId, targetServicePath),
  );
  await agenda.cancel(
    buildFDAJobFilter('clean-partition', service, fdaId, targetServicePath),
  );
}

export async function getDAs(service, fdaId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  return retrieveDAs(service, fdaId, servicePath);
}

export async function getDA(service, fdaId, daId, visibility, servicePath) {
  if (visibility !== undefined || servicePath !== undefined) {
    await getAccessibleFDA(service, fdaId, visibility, servicePath);
  }

  const da = await retrieveDA(service, fdaId, daId, servicePath);
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
    await validateDAQuery(conn, service, fdaId, userQuery, servicePath);
    await updateDA(
      service,
      fdaId,
      servicePath,
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

  await removeDA(service, fdaId, daId, servicePath);
}

export async function cleanPartition(
  service,
  fdaId,
  windowSize,
  objStgConf,
  servicePath,
) {
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
    /* c8 ignore next 5 */
    throw new FDAError(
      400,
      'CleaningError',
      'Incorrect window size in refresh policy.',
    );
  }

  /* c8 ignore next 6 */
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  const bucketName = getBucketNameFromService(service);

  /* c8 ignore next 6 */
  const objPaths = await listObjects(
    s3Client,
    bucketName,
    getFDAStoragePath(fdaId, servicePath),
  );

  const partitionsToRemove = [];
  for (const path of objPaths) {
    const partitionDate = extractDate(path);

    if (partitionDate < cutoff) {
      partitionsToRemove.push(path);
    }
  }
  await dropFiles(s3Client, bucketName, partitionsToRemove);
}

async function uploadTableToObjStg(
  service,
  database,
  query,
  bucket,
  path,
  fdaId,
  servicePath,
  timeColumn,
  objStgConf,
  partitionFlag,
) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  await updateFDAStatus(service, fdaId, servicePath, 'fetching', 20);
  await uploadTable(s3Client, bucket, database, query, path);

  const conn = await getDBConnection();
  try {
    await updateFDAStatus(service, fdaId, servicePath, 'transforming', 60);

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

    await updateFDAStatus(service, fdaId, servicePath, 'uploading', 80);
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

async function getStoredFDA(service, fdaId, servicePath) {
  const fda = await retrieveFDA(service, fdaId, servicePath);

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
  const fda = await retrieveFDA(service, fdaId, normalizedServicePath);

  if (!fda) {
    const sameIdCandidates = (await retrieveFDAs(service)).filter(
      (candidate) => candidate.fdaId === fdaId,
    );

    if (sameIdCandidates.length === 0) {
      throw new FDAError(
        404,
        'FDANotFound',
        `FDA ${fdaId} not found in service ${service}`,
      );
    }

    const hasMatchingVisibility = sameIdCandidates.some(
      (candidate) =>
        normalizeVisibility(candidate.visibility) === normalizedVisibility,
    );

    if (!hasMatchingVisibility) {
      throw new FDAError(
        403,
        'VisibilityMismatch',
        `FDA ${fdaId} does not belong to ${normalizedVisibility}`,
      );
    }

    throw new FDAError(
      403,
      'ServicePathMismatch',
      `FDA ${fdaId} does not belong to servicePath ${normalizedServicePath}`,
    );
  }

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
  try {
    return normalizeScopedServicePath(servicePath);
  } catch (error) {
    if (error.message === 'servicePath is required') {
      throw new FDAError(
        400,
        'InvalidServicePath',
        'Fiware-ServicePath header is required',
      );
    }

    throw new FDAError(
      400,
      'InvalidServicePath',
      'Fiware-ServicePath must be a non-root absolute path (e.g. /servicepath)',
    );
  }
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

async function createOneRowParquetSync(service, fdaId, query, servicePath) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  const storagePath = getFDAStoragePath(fdaId, servicePath);
  const bucketName = getBucketNameFromService(service);

  const oneRowQuery = buildOneRowQuery(query);
  await uploadTable(s3Client, bucketName, service, oneRowQuery, storagePath);

  const conn = await getDBConnection();
  try {
    const parquetPath = getPath(bucketName, storagePath, '.parquet');
    await toParquet(
      conn,
      getPath(bucketName, storagePath, '.csv'),
      parquetPath,
    );
    await dropFile(s3Client, bucketName, `${storagePath}.csv`);
  } finally {
    await releaseDBConnection(conn);
  }
}

function buildOneRowQuery(query) {
  const normalizedQuery = query.trim().replace(/;+\s*$/, '');
  return `SELECT * FROM (${normalizedQuery}) AS fda_one_row LIMIT 1`;
}

async function buildDefaultDataAccessDefinition(
  service,
  fdaId,
  servicePath,
  timeColumn,
) {
  const columns = await getFDAColumnNamesFromParquet(
    service,
    fdaId,
    servicePath,
  );

  const resolvedTimeColumn = resolveDefaultDATimeColumnName(
    timeColumn,
    columns,
  );
  const reservedParamNames = ['limit', 'offset'];
  if (resolvedTimeColumn) {
    reservedParamNames.push('start', 'finish');
  }

  if (columns.length === 0) {
    const params = reservedParamNames.map((name) => ({
      name,
      default: null,
    }));

    return {
      query:
        'SELECT * LIMIT CAST(COALESCE($limit, 9223372036854775807) AS BIGINT) OFFSET CAST(COALESCE($offset, 0) AS BIGINT)',
      params,
    };
  }

  const usedParamNames = new Set(reservedParamNames);
  const params = [];
  const filters = [];

  for (const columnName of columns) {
    const baseName = sanitizeDefaultDAParamBaseName(columnName);
    const paramName = getUniqueDefaultDAParamName(baseName, usedParamNames);
    const quotedColumnName = quoteDuckDBIdentifier(columnName);
    const isResolvedTimeColumn =
      resolvedTimeColumn && columnName === resolvedTimeColumn;

    params.push({ name: paramName, default: null });
    if (isResolvedTimeColumn) {
      filters.push(
        `($${paramName} IS NULL OR DATE_TRUNC('millisecond', CAST(${quotedColumnName} AS TIMESTAMP)) = DATE_TRUNC('millisecond', CAST($${paramName} AS TIMESTAMP)))`,
      );
    } else {
      filters.push(
        `($${paramName} IS NULL OR ${quotedColumnName} = $${paramName})`,
      );
    }
  }

  if (resolvedTimeColumn) {
    const quotedTimeColumn = quoteDuckDBIdentifier(resolvedTimeColumn);
    params.push({ name: 'start', default: null });
    params.push({ name: 'finish', default: null });
    filters.push(
      `($start IS NULL OR CAST(${quotedTimeColumn} AS TIMESTAMP) >= CAST($start AS TIMESTAMP))`,
    );
    filters.push(
      `($finish IS NULL OR CAST(${quotedTimeColumn} AS TIMESTAMP) <= CAST($finish AS TIMESTAMP))`,
    );
  }

  params.push({ name: 'limit', default: null });
  params.push({ name: 'offset', default: null });

  const whereClause =
    filters.length > 0 ? ` WHERE ${filters.join(' AND ')}` : '';

  return {
    query: `SELECT *${whereClause} LIMIT CAST(COALESCE($limit, 9223372036854775807) AS BIGINT) OFFSET CAST(COALESCE($offset, 0) AS BIGINT)`,
    params,
  };
}

function resolveDefaultDATimeColumnName(timeColumn, columns) {
  if (typeof timeColumn !== 'string' || timeColumn.length === 0) {
    return null;
  }

  const exactMatch = columns.find((column) => column === timeColumn);
  if (exactMatch) {
    return exactMatch;
  }

  const lowerTimeColumn = timeColumn.toLowerCase();
  return (
    columns.find(
      (column) =>
        typeof column === 'string' && column.toLowerCase() === lowerTimeColumn,
    ) || null
  );
}

async function getFDAColumnNamesFromParquet(service, fdaId, servicePath) {
  const conn = await getDBConnection();
  try {
    const storagePath = getFDAStoragePath(fdaId, servicePath);
    const bucketName = getBucketNameFromService(service);
    const parquetPath = `s3://${bucketName}/${storagePath}.parquet`;
    const safeParquetPath = parquetPath.replace(/'/g, "''");

    const describeResult = await conn.run(
      `DESCRIBE SELECT * FROM read_parquet('${safeParquetPath}')`,
    );
    const describeRows = await Promise.resolve(
      describeResult.getRowObjectsJson(),
    );

    return describeRows
      .map(
        (row) =>
          row?.column_name ?? row?.columnName ?? row?.name ?? row?.column,
      )
      .filter((name) => typeof name === 'string' && name.length > 0);
  } finally {
    await releaseDBConnection(conn);
  }
}

function quoteDuckDBIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function sanitizeDefaultDAParamBaseName(columnName) {
  const normalized = String(columnName)
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  if (!normalized) {
    return 'col';
  }

  if (/^[0-9]/.test(normalized)) {
    return `col_${normalized}`;
  }

  return normalized;
}

function getUniqueDefaultDAParamName(baseName, usedParamNames) {
  let candidate = baseName;
  let suffix = 2;

  while (usedParamNames.has(candidate)) {
    candidate = `${baseName}_${suffix}`;
    suffix += 1;
  }

  usedParamNames.add(candidate);
  return candidate;
}

async function rollbackFDAProvisioning(service, fdaId, servicePath) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  const storagePath = getFDAStoragePath(fdaId, servicePath);
  const bucketName = getBucketNameFromService(service);

  const rollbackResults = await Promise.allSettled([
    dropFile(s3Client, bucketName, `${storagePath}.csv`),
    dropFile(s3Client, bucketName, `${storagePath}.parquet`),
    removeFDA(service, fdaId, servicePath),
  ]);

  const mongoRollbackResult = rollbackResults[2];
  if (mongoRollbackResult.status === 'rejected') {
    throw mongoRollbackResult.reason;
  }
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};
