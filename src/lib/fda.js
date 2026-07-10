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
  validateDAParamBindings,
  resolveDAParams,
  validateDAQuery,
  extractDate,
  PARTITION_TYPES,
  refreshIntervalPartitionCheck,
} from './utils/db.js';
import {
  uploadTable,
  runPgQuery,
  createPgCursorReader,
  validatePostgresDatasourceConnection,
  validatePostgresQuery,
} from './utils/pg.js';
import {
  getS3Client,
  newUpload,
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
  createDatasource,
  retrieveDatasources,
  retrieveDatasource,
  updateDatasource,
  removeDatasource,
  countFDAsUsingDatasource,
  validateMongoDatasourceConnection,
  readMongoDatasourceRows,
} from './utils/mongo.js';
import {
  normalizeForSerialization,
  getWindowDate,
  assertFreshQueriesEnabled,
  acquireFreshQuerySlot,
  convertRefreshIntervalToMs,
  getTimeColumnQuery,
} from './utils/utils.js';
import {
  buildFDAJobFilter,
  buildFDAJobCancelFilter,
  getBucketNameFromService,
  getFDAStoragePath,
  normalizeScopedServicePath,
} from './utils/fdaScope.js';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

const FRESH_CURSOR_BATCH_SIZE = 250;

const DEFAULT_DATASOURCE_ID = 'default';
const SUPPORTED_DATASOURCE_TYPES = new Set(['postgres', 'mongodb']);

function assertSupportedDatasourceType(type) {
  if (!SUPPORTED_DATASOURCE_TYPES.has(type)) {
    throw new FDAError(
      400,
      'UnsupportedDatasourceType',
      `Datasource type ${type} is not supported for this operation`,
    );
  }
}

function validateMongoFDAContract(query, timeColumn, cached) {
  validateBasicQueryStructure(query);

  const { collection, filter, projection, aggregation } = query;
  validateCollection(collection);

  const queryType = determineQueryType(filter, aggregation);

  if (queryType === 'find') {
    validateFindQuery(filter, projection, timeColumn);
  } else if (queryType === 'aggregation') {
    validateAggregationQuery(aggregation);
  }

  validateCacheSupport(cached);
}

// Helper functions to reduce complexity
function validateBasicQueryStructure(query) {
  if (!query || typeof query !== 'object' || Array.isArray(query)) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA query must be a JSON object',
    );
  }
}

function validateCollection(collection) {
  if (!collection || typeof collection !== 'string') {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA query requires a non-empty collection field',
    );
  }
}

function determineQueryType(filter, aggregation) {
  const hasFindQuery = filter !== undefined;
  const hasAggregationQuery = aggregation !== undefined;

  if (hasFindQuery === hasAggregationQuery) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA query must define either filter or aggregation',
    );
  }

  return hasFindQuery ? 'find' : 'aggregation';
}

function validateFindQuery(filter, projection, timeColumn) {
  validateFilter(filter);
  validateProjection(projection);
  validateTimeColumnInProjection(timeColumn, projection);
}

function validateFilter(filter) {
  if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA filter must be a JSON object',
    );
  }
}

function validateProjection(projection) {
  if (
    projection !== undefined &&
    (projection === null ||
      typeof projection !== 'object' ||
      Array.isArray(projection))
  ) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA projection must be an object',
    );
  }
}

function validateTimeColumnInProjection(timeColumn, projection) {
  if (timeColumn && projection && !(timeColumn in projection)) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA timeColumn must be included in projection',
    );
  }
}

function validateAggregationQuery(aggregation) {
  if (!Array.isArray(aggregation) || aggregation.length === 0) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo FDA aggregation must be a non-empty array',
    );
  }
  throw new FDAError(
    400,
    'MongoAggregationNotSupported',
    'Aggregation pipelines are not supported yet',
  );
}

function validateCacheSupport(cached) {
  if (cached === false) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo datasource only supports cached FDAs',
    );
  }
}

async function resolveDatasource(service, datasourceId) {
  const ds = await retrieveDatasource(service, datasourceId);
  if (!ds) {
    throw new FDAError(
      404,
      'DatasourceNotFound',
      `Datasource ${datasourceId} not found for service ${service}`,
    );
  }

  assertSupportedDatasourceType(ds.type);
  return ds;
}

async function resolveDatasourceCredentials(service, datasourceId) {
  const ds = await resolveDatasource(service, datasourceId);
  return ds.config;
}

async function validateDatasourceConnection(type, dsConfig) {
  assertSupportedDatasourceType(type);

  if (type === 'postgres') {
    await validatePostgresDatasourceConnection(dsConfig);
  } else {
    await validateMongoDatasourceConnection(dsConfig);
  }
}

export async function createDatasourceForService(
  service,
  datasourceId,
  type,
  dsConfig,
) {
  await validateDatasourceConnection(type, dsConfig);
  await createDatasource(service, datasourceId, type, dsConfig);
}

export function getDatasourcesForService(service) {
  return retrieveDatasources(service);
}

export async function getDatasourceForService(service, datasourceId) {
  const ds = await retrieveDatasource(service, datasourceId);
  if (!ds) {
    throw new FDAError(
      404,
      'DatasourceNotFound',
      `Datasource ${datasourceId} not found for service ${service}`,
    );
  }
  return ds;
}

export async function updateDatasourceForService(
  service,
  datasourceId,
  type,
  dsConfig,
) {
  if (type !== undefined || dsConfig !== undefined) {
    const current = await getDatasourceForService(service, datasourceId);
    await validateDatasourceConnection(
      type ?? current.type,
      dsConfig ?? current.config,
    );
  }

  await updateDatasource(service, datasourceId, type, dsConfig);
}

export async function deleteDatasourceForService(service, datasourceId) {
  const usedBy = await countFDAsUsingDatasource(service, datasourceId);
  if (usedBy > 0) {
    throw new FDAError(
      409,
      'DatasourceInUse',
      `Datasource ${datasourceId} is being used by ${usedBy} FDA(s) in service ${service}`,
    );
  }

  await removeDatasource(service, datasourceId);
}
export const VALID_VISIBILITIES = ['public', 'private'];
const VALID_VISIBILITIES_SET = new Set(VALID_VISIBILITIES);
const VALID_REFRESH_POLICY_TYPES = ['none', 'interval', 'window'];
const VALID_WINDOW_FETCH_SIZES = ['hour', 'day', 'week', 'month', 'year'];
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

export async function executeFDAQuery({
  service,
  visibility,
  servicePath,
  fdaId,
}) {
  assertFreshQueriesEnabled(config.roles.syncQueries);

  const releaseFreshSlot = acquireFreshQuerySlot(
    config.freshQueries.maxConcurrent,
  );
  try {
    const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);
    const query = buildFreshQueryFromFDA(fda, fdaId);
    const pgCredentials = await resolveDatasourceCredentials(
      service,
      fda.datasourceId ?? DEFAULT_DATASOURCE_ID,
    );
    const rows = await runPgQuery(pgCredentials, query, []);
    return normalizeForSerialization(rows);
  } finally {
    releaseFreshSlot();
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

export async function executeFDAQueryStream({
  service,
  visibility,
  servicePath,
  fdaId,
  req,
  res,
  format = 'ndjson',
}) {
  const source = await createFreshFDARowSource({
    service,
    visibility,
    servicePath,
    fdaId,
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
          await writeNdjsonLine(res, row);
        }
        continue;
      }

      if (!csvColumns) {
        csvColumns = Object.keys(rows[0] ?? {});
        await writeCsvHeader(res, csvColumns);
      }

      for (const row of rows) {
        const csvLine =
          csvColumns.map((column) => escapeCsvValue(row[column])).join(',') +
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
    const { text, values, fda } = await buildFreshQueryStatement(
      service,
      visibility,
      servicePath,
      params,
    );
    const pgCredentials = await resolveDatasourceCredentials(
      service,
      fda.datasourceId ?? DEFAULT_DATASOURCE_ID,
    );

    cursorReader = await createPgCursorReader(
      pgCredentials,
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

async function createFreshFDARowSource({
  service,
  visibility,
  servicePath,
  fdaId,
  req,
}) {
  assertFreshQueriesEnabled(config.roles.syncQueries);

  const releaseFreshSlot = acquireFreshQuerySlot(
    config.freshQueries.maxConcurrent,
  );
  let cursorReader;

  try {
    const { query, fda } = await buildFreshFDAQuery(
      service,
      visibility,
      servicePath,
      fdaId,
    );
    const pgCredentials = await resolveDatasourceCredentials(
      service,
      fda.datasourceId ?? DEFAULT_DATASOURCE_ID,
    );

    cursorReader = await createPgCursorReader(
      pgCredentials,
      query,
      [],
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
    const { text, values, fda } = await buildFreshQueryStatement(
      service,
      visibility,
      servicePath,
      params,
    );
    const pgCredentials = await resolveDatasourceCredentials(
      service,
      fda.datasourceId ?? DEFAULT_DATASOURCE_ID,
    );
    const rows = await runPgQuery(pgCredentials, text, values);
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

  return {
    ...replaceNamedParamsWithPositional(freshBaseQuery, validatedParams),
    fda,
  };
}

async function buildFreshFDAQuery(service, visibility, servicePath, fdaId) {
  const fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);
  return { query: buildFreshQueryFromFDA(fda, fdaId), fda };
}

function buildFreshQueryFromFDA(fda, fdaId) {
  if (fda.cached !== false) {
    throw new FDAError(
      409,
      'FDANotOnlyFresh',
      `FDA ${fdaId} is a cached FDA and cannot be queried directly. Use a Data Access instead.`,
    );
  }
  return removeTrailingSemicolon(fda.query?.trim() || '');
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

  const text = query.replaceAll(/\$([A-Za-z_]\w*)/g, (_m, name) => {
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
    let fda;
    if (visibility !== undefined || servicePath !== undefined) {
      fda = await getAccessibleFDA(service, fdaId, visibility, servicePath);
      assertFDAIsCached(fda, fdaId);
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
    validateDAParamBindings(userQuery, normalizedParams || []);
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
  cached = true,
  datasourceId = DEFAULT_DATASOURCE_ID,
  skipBootstrap = false,
) {
  const normalizedVisibility = normalizeVisibility(visibility);
  const normalizedServicePath = normalizeServicePath(servicePath);
  const datasource = await getDatasourceForService(service, datasourceId);
  validateScheduledOptions(refreshPolicy, objStgConf);

  if (datasource.type === 'mongodb') {
    validateMongoFDAContract(query, timeColumn, cached);

    if (refreshPolicy?.type === 'window') {
      throw new FDAError(
        400,
        'InvalidMongoFDAContract',
        'Mongo datasource does not support window refresh policy',
      );
    }
  }

  const timeQuery =
    datasource.type === 'postgres' &&
    (refreshPolicy?.type === 'window' || objStgConf?.partition)
      ? getTimeColumnQuery(query, timeColumn)
      : query;

  let sourceSchema;

  if (datasource.type === 'postgres') {
    sourceSchema = await validatePostgresQuery(datasource.config, timeQuery, {
      timeColumn,
      returnColumns: true,
    });
  }

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
    cached,
    datasourceId,
  );

  if (!skipBootstrap) {
    if (cached) {
      try {
        await createOneRowParquetSync(
          service,
          fdaId,
          timeQuery,
          normalizedServicePath,
          datasourceId,
          timeColumn,
          objStgConf,
          sourceSchema,
        );
      } catch (err) {
        await rollbackFDAProvisioning(service, fdaId, normalizedServicePath);
        throw err;
      }
    }

    await createDefaultDAIfNeeded({
      cached,
      defaultDataAccessEnabled,
      service,
      fdaId,
      normalizedServicePath,
      timeColumn,
      objStgConf,
      normalizedVisibility,
      sourceSchema,
    });
  }

  if (!cached) {
    return;
  }

  const agenda = getAgenda();

  let firstQuery = timeQuery;
  let recurringQuery = timeQuery;
  if (datasource.type === 'postgres' && refreshPolicy?.type === 'window') {
    firstQuery = getWindowQuery(
      timeQuery,
      timeColumn,
      refreshPolicy?.params?.windowSize,
    );
    recurringQuery = getWindowQuery(
      timeQuery,
      timeColumn,
      refreshPolicy?.params?.fetchSize,
    );
  } else if (
    datasource.type === 'mongodb' &&
    refreshPolicy?.type === 'window'
  ) {
    throw new FDAError(
      400,
      'InvalidMongoFDAContract',
      'Mongo datasource does not support window refresh policy',
    );
  }

  // Execute first fetch immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: firstQuery,
    service,
    servicePath: normalizedServicePath,
    timeColumn,
    refreshPolicy,
    objStgConf,
    datasourceId,
  });

  // Schedule refreshes according to policy
  if (refreshPolicy?.type === 'interval' || refreshPolicy?.type === 'window') {
    await scheduleFDAJobs({
      agenda,
      fdaId,
      query: recurringQuery,
      service,
      servicePath: normalizedServicePath,
      timeColumn,
      refreshPolicy,
      objStgConf,
      datasourceId,
    });
  }
}

function getWindowQuery(query, timeColumn, startDate) {
  if (!startDate) {
    return query;
  }
  const prevWindowStartDate = getPreviousWindowStartDate(startDate);
  return getUpdateWindowQuery(query, timeColumn, prevWindowStartDate);
}

function validateScheduledOptions(refreshPolicy, objStgConf) {
  if (!refreshPolicy) {
    return;
  }

  if (!VALID_REFRESH_POLICY_TYPES.includes(refreshPolicy.type)) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid refresh policy type "${refreshPolicy.type}".`,
    );
  }

  if (refreshPolicy.type === 'none') {
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

  if (convertRefreshIntervalToMs(refreshInterval) === null) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid refresh interval "${refreshInterval}".`,
    );
  }

  if (refreshPolicy.type === 'window' && !fetchSize) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Missing required refresh policy parameter: fetchSize.`,
    );
  }

  if (
    refreshPolicy.type === 'window' &&
    fetchSize &&
    !VALID_WINDOW_FETCH_SIZES.includes(fetchSize)
  ) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid fetchSize "${fetchSize}".`,
    );
  }

  if (
    refreshPolicy.type === 'window' &&
    refreshPolicy.params?.windowSize &&
    !objStgConf?.partition
  ) {
    throw new FDAError(
      400,
      'InvalidParam',
      'windowSize requires objStgConf.partition.',
    );
  }

  if (
    refreshPolicy.params?.windowSize &&
    !getWindowDate(refreshPolicy.params.windowSize)
  ) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid windowSize "${refreshPolicy.params.windowSize}".`,
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
  const fda =
    visibility !== undefined
      ? await getAccessibleFDA(service, fdaId, visibility, servicePath)
      : await getStoredFDA(service, fdaId, normalizedServicePath);

  assertFDAIsCached(fda, fdaId);

  const previous = await regenerateFDA(service, fdaId, normalizedServicePath);

  const agenda = getAgenda();

  // Execute refresh immediately (when a fetcher is free)
  const effectiveServicePath = previous.servicePath ?? normalizedServicePath;

  let firstQuery = previous.query;
  if (previous.refreshPolicy?.type === 'window') {
    firstQuery = getWindowQuery(
      previous.query,
      previous.timeColumn,
      previous.refreshPolicy?.params?.windowSize,
    );
  }
  await agenda.now('refresh-fda', {
    fdaId,
    query: firstQuery,
    service,
    servicePath: effectiveServicePath,
    timeColumn: previous.timeColumn,
    refreshPolicy: previous.refreshPolicy,
    objStgConf: previous.objStgConf,
    datasourceId: previous.datasourceId ?? DEFAULT_DATASOURCE_ID,
  });

  if (previous.refreshPolicy?.params?.windowSize) {
    await agenda.now('clean-partition', {
      fdaId,
      service,
      servicePath: effectiveServicePath,
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
  datasourceId = DEFAULT_DATASOURCE_ID,
) {
  const storagePath = getFDAStoragePath(fdaId, servicePath);
  const bucketName = getBucketNameFromService(service);

  try {
    await updateFDAStatus(service, fdaId, servicePath, 'fetching', 10);

    await uploadTableToObjStg(
      service,
      datasourceId,
      query,
      bucketName,
      storagePath,
      fdaId,
      servicePath,
      timeColumn,
      objStgConf,
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
    case 'hour': {
      const d = new Date(now);
      d.setUTCHours(d.getUTCHours() - 1, 0, 0, 0);
      return d.toISOString();
    }

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

function buildCsvContentFromRows(rows, columns) {
  const header = columns.map((column) => escapeCsvValue(column)).join(',');

  if (rows.length === 0) {
    return `${header}\n`;
  }

  const dataLines = rows.map((row) =>
    columns.map((column) => escapeCsvValue(row[column])).join(','),
  );

  return `${header}\n${dataLines.join('\n')}\n`;
}

async function uploadCsvContentToObjectStorage(s3Client, bucket, path, body) {
  const upload = newUpload(s3Client, bucket, `${path}.csv`, body, 5, 1);

  try {
    await upload.done();
  } catch (error) {
    throw new FDAError(
      503,
      'UploadError',
      `Error uploading FDA to object storage: ${error.message}`,
    );
  }
}

async function getMongoFDAStorageRows(
  service,
  datasourceId,
  fdaId,
  servicePath,
  { limit } = {},
) {
  const datasource = await resolveDatasource(service, datasourceId);
  const fda = await retrieveFDA(service, fdaId, servicePath);

  if (!fda) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} not found in service ${service}`,
    );
  }

  validateMongoFDAContract(fda.query, fda.timeColumn, fda.cached);

  return {
    columns: Object.keys(fda.query.projection),
    rows: await readMongoDatasourceRows(datasource.config, fda.query, {
      limit,
    }),
  };
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
  const storagePath = getFDAStoragePath(fdaId, targetServicePath);
  const allObjPaths = await listObjects(s3Client, bucketName, storagePath);
  // Filter strictly to objects belonging to this FDA only, preventing
  // accidental deletion of sibling FDAs whose IDs share a common prefix
  // (e.g. fda_test and fda_test_1 both match the prefix "fda_test").
  const objPaths = allObjPaths.filter(
    (key) =>
      key.startsWith(`${storagePath}/`) || key.startsWith(`${storagePath}.`),
  );
  await dropFiles(s3Client, bucketName, objPaths);

  await removeFDA(service, fdaId, targetServicePath);

  const agenda = getAgenda();
  await agenda.cancel(
    buildFDAJobCancelFilter(
      'refresh-fda-recurring',
      service,
      fdaId,
      targetServicePath,
    ),
  );
  await agenda.cancel(
    buildFDAJobCancelFilter(
      'clean-partition-recurring',
      service,
      fdaId,
      targetServicePath,
    ),
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
    validateDAParamBindings(userQuery, normalizedParams || []);
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

  /* c8 ignore next 10 */
  const cleanPartitionStoragePath = getFDAStoragePath(fdaId, servicePath);
  const allPartitionPaths = await listObjects(
    s3Client,
    bucketName,
    cleanPartitionStoragePath,
  );
  // Filter strictly to objects belonging to this FDA only (same prefix-collision guard as deleteFDA)
  const objPaths = allPartitionPaths.filter((key) =>
    key.startsWith(`${cleanPartitionStoragePath}/`),
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
  datasourceId,
  query,
  bucket,
  path,
  fdaId,
  servicePath,
  timeColumn,
  objStgConf,
) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  const datasource = await resolveDatasource(service, datasourceId);
  await updateFDAStatus(service, fdaId, servicePath, 'fetching', 20);

  if (datasource.type === 'postgres') {
    await uploadTable(s3Client, bucket, datasource.config, query, path);
  } else {
    const { columns, rows } = await getMongoFDAStorageRows(
      service,
      datasourceId,
      fdaId,
      servicePath,
    );
    const csvContent = buildCsvContentFromRows(rows, columns);
    await uploadCsvContentToObjectStorage(s3Client, bucket, path, csvContent);
  }

  const conn = await getDBConnection();
  try {
    await updateFDAStatus(service, fdaId, servicePath, 'transforming', 60);

    // DuckDB cant overwrite files in Minio, so for partitioned files we upload them in a tmp file and the move them
    // This includes first upload because the one row parquet is also partitioned
    const parquetPath = objStgConf?.partition
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

    if (objStgConf?.partition) {
      const objectsList = await listObjects(
        s3Client,
        bucket,
        `tmp/${path}.parquet`,
      );
      const hasRealPartitionedParquet = objectsList.some((key) =>
        key.endsWith('.parquet'),
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

      if (hasRealPartitionedParquet) {
        await dropFile(
          s3Client,
          bucket,
          `${path}.parquet/${getSchemaPartitionPath(objStgConf.partition)}/schema.parquet`,
        );
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
  assertFDAIsCached(fda, fdaId);

  // Queries are blocked only before the first successful fetch.
  if (!fda.lastFetch) {
    throw new FDAError(
      409,
      'FDAUnavailable',
      `FDA ${fdaId} is not queryable yet because the first fetch has not completed`,
    );
  }
}

export async function getStoredFDA(service, fdaId, servicePath) {
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

async function createOneRowParquetSync(
  service,
  fdaId,
  query,
  servicePath,
  datasourceId,
  timeColumn,
  objStgConf,
  sourceSchema,
) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  const storagePath = getFDAStoragePath(fdaId, servicePath);
  const bucketName = getBucketNameFromService(service);
  const datasource = await resolveDatasource(service, datasourceId);

  if (datasource.type === 'postgres') {
    const oneRowQuery = buildOneRowQuery(query);
    await uploadTable(
      s3Client,
      bucketName,
      datasource.config,
      oneRowQuery,
      storagePath,
    );
  } else {
    const { columns, rows } = await getMongoFDAStorageRows(
      service,
      datasourceId,
      fdaId,
      servicePath,
      { limit: 1 },
    );
    const csvContent = buildCsvContentFromRows(rows, columns);
    await uploadCsvContentToObjectStorage(
      s3Client,
      bucketName,
      storagePath,
      csvContent,
    );
  }

  const conn = await getDBConnection();
  try {
    const parquetPath = getPath(bucketName, storagePath, '.parquet');
    await toParquet(
      conn,
      getPath(bucketName, storagePath, '.csv'),
      parquetPath,
      timeColumn,
      objStgConf?.partition,
    );

    if (datasource.type === 'postgres' && objStgConf?.partition) {
      const partitionPrefix = `${storagePath}.parquet`;
      const parquetFiles = await listObjects(
        s3Client,
        bucketName,
        partitionPrefix,
      );
      const normalizedParquetFiles = Array.isArray(parquetFiles)
        ? parquetFiles
        : [];
      const hasPartitionedDataParquet = normalizedParquetFiles.some(
        (key) =>
          key.startsWith(`${partitionPrefix}/`) && key.endsWith('.parquet'),
      );

      if (!hasPartitionedDataParquet && typeof conn?.run === 'function') {
        await createSchemaParquetForEmptyPartitionedFDA(
          conn,
          bucketName,
          storagePath,
          sourceSchema,
        );
      }
    }

    await dropFile(s3Client, bucketName, `${storagePath}.csv`);
  } finally {
    await releaseDBConnection(conn);
  }
}

function buildOneRowQuery(query) {
  const normalizedQuery = query.trim().replace(/;+\s*$/, '');

  // Schema-only bootstrap keeps creation validation synchronous without row materialization.
  return `SELECT * FROM (${normalizedQuery}) AS fda_one_row LIMIT 0`;
}

async function buildDefaultDataAccessDefinition(
  service,
  fdaId,
  servicePath,
  timeColumn,
  objStgConf,
  schemaOverride,
) {
  let overrideColumns = [];

  if (Array.isArray(schemaOverride?.columns)) {
    overrideColumns = schemaOverride.columns;
  } else if (Array.isArray(schemaOverride)) {
    overrideColumns = schemaOverride;
  }

  const normalizedOverrideColumns = overrideColumns.filter(
    (name) => typeof name === 'string' && name.length > 0,
  );

  const columns =
    normalizedOverrideColumns.length > 0
      ? normalizedOverrideColumns
      : await getFDAColumnNamesFromParquet(
          service,
          fdaId,
          servicePath,
          objStgConf,
        );

  const resolvedTimeColumn = resolveDefaultDATimeColumnName(
    timeColumn,
    columns,
  );
  const reservedParamNames = ['pageSize', 'pageStart'];
  if (resolvedTimeColumn) {
    reservedParamNames.push('start', 'finish');
  }

  if (columns.length === 0) {
    const params = reservedParamNames.map((name) => {
      if (name === 'pageSize') {
        return { name, default: '9223372036854775807' };
      }

      if (name === 'pageStart') {
        return { name, default: 0 };
      }

      return { name, default: null };
    });

    return {
      query:
        'SELECT *, COUNT(*) OVER() as __total LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
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

  params.push({ name: 'pageSize', default: '9223372036854775807' });
  params.push({ name: 'pageStart', default: 0 });

  const whereClause =
    filters.length > 0 ? ` WHERE ${filters.join(' AND ')}` : '';

  return {
    query: `SELECT *, COUNT(*) OVER() as __total${whereClause} LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)`,
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

async function getFDAColumnNamesFromParquet(
  service,
  fdaId,
  servicePath,
  objStgConf,
) {
  const conn = await getDBConnection();
  try {
    const storagePath = getFDAStoragePath(fdaId, servicePath);
    const bucketName = getBucketNameFromService(service);
    const parquetPath = objStgConf?.partition
      ? `s3://${bucketName}/${storagePath}.parquet/**/*.parquet`
      : `s3://${bucketName}/${storagePath}.parquet`;
    const schemaBootstrapParquetPath = `s3://${bucketName}/${storagePath}.__schema__.parquet`;
    const safeParquetPath = parquetPath.replaceAll("'", "''");
    const safeSchemaBootstrapParquetPath =
      schemaBootstrapParquetPath.replaceAll("'", "''");

    let describeResult;
    try {
      describeResult = await conn.run(
        `DESCRIBE SELECT * FROM read_parquet('${safeParquetPath}')`,
      );
    } catch (error) {
      if (objStgConf?.partition && isParquetFilesMissingError(error)) {
        describeResult = await conn.run(
          `DESCRIBE SELECT * FROM read_parquet('${safeSchemaBootstrapParquetPath}')`,
        );
      } else {
        throw error;
      }
    }

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

function isParquetFilesMissingError(error) {
  return String(error?.message ?? error).includes(
    'No files found that match the pattern',
  );
}

async function createSchemaParquetForEmptyPartitionedFDA(
  conn,
  bucketName,
  storagePath,
  sourceSchema,
) {
  const schemaParquetPath = `s3://${bucketName}/${storagePath}.__schema__.parquet`;
  const safeSchemaParquetPath = schemaParquetPath.replaceAll("'", "''");
  const schemaFields = Array.isArray(sourceSchema?.fields)
    ? sourceSchema.fields.filter(
        (field) =>
          typeof field?.name === 'string' &&
          field.name.length > 0 &&
          typeof field?.duckdbType === 'string' &&
          field.duckdbType.length > 0,
      )
    : [];

  if (schemaFields.length === 0) {
    return;
  }

  const selectList = schemaFields
    .map(
      ({ name, duckdbType }) =>
        `CAST(NULL AS ${duckdbType}) AS ${quoteDuckDBIdentifier(name)}`,
    )
    .join(', ');

  await conn.run(
    `COPY (SELECT ${selectList} LIMIT 0) TO '${safeSchemaParquetPath}' (FORMAT PARQUET);`,
  );
}

function getSchemaPartitionPath(partitionType) {
  const partitionPaths = {
    day: 'year=9999/month=12/day=31',
    week: 'year=9999/week=9999-52',
    month: 'year=9999/month=12',
    year: 'year=9999',
  };

  return partitionPaths[partitionType] ?? partitionPaths.year;
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

function assertFDAIsCached(fda, fdaId) {
  if (fda?.cached === false) {
    throw new FDAError(
      409,
      'FDAOnlyFresh',
      `FDA ${fdaId} is configured as only-fresh and does not support this operation.`,
    );
  }
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};

async function scheduleFDAJobs({
  agenda,
  fdaId,
  query,
  service,
  servicePath,
  timeColumn,
  refreshPolicy,
  objStgConf,
  datasourceId,
}) {
  const { refreshInterval, windowSize } = refreshPolicy.params || {};

  const refreshJob = agenda.create('refresh-fda-recurring', {
    fdaId,
    query,
    service,
    servicePath,
    timeColumn,
    refreshPolicy,
    objStgConf,
    datasourceId,
  });

  refreshJob.unique(
    buildFDAJobFilter('refresh-fda-recurring', service, fdaId, servicePath),
  );
  refreshJob.repeatEvery(refreshInterval, { skipImmediate: true });
  await refreshJob.save();

  if (windowSize) {
    const cleanPartitionJob = agenda.create('clean-partition-recurring', {
      fdaId,
      service,
      servicePath,
      windowSize,
      objStgConf,
    });

    cleanPartitionJob.unique(
      buildFDAJobFilter(
        'clean-partition-recurring',
        service,
        fdaId,
        servicePath,
      ),
    );
    cleanPartitionJob.repeatEvery(refreshInterval, { skipImmediate: true });
    await cleanPartitionJob.save();
  }
}

async function createDefaultDAIfNeeded({
  cached,
  defaultDataAccessEnabled,
  service,
  fdaId,
  normalizedServicePath,
  timeColumn,
  objStgConf,
  normalizedVisibility,
  sourceSchema,
}) {
  if (!cached || !defaultDataAccessEnabled) {
    return;
  }

  try {
    const defaultDA = await buildDefaultDataAccessDefinition(
      service,
      fdaId,
      normalizedServicePath,
      timeColumn,
      objStgConf,
      sourceSchema,
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
    await rollbackFDAProvisioning(service, fdaId, normalizedServicePath);
    throw new FDAError(
      500,
      'DefaultDataAccessCreationError',
      `Failed to create default Data Access for FDA ${fdaId}: ${err.message}`,
    );
  }
}
