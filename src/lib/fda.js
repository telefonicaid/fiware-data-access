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
import { promisify } from 'node:util';
import Cursor from 'pg-cursor';
import {
  runPreparedStatement,
  runPreparedStatementStream,
  getDBConnection,
  releaseDBConnection,
  toParquet,
  checkParams,
  buildDAQuery,
  resolveDAParams,
} from './utils/db.js';
import { uploadTable, getPgClient } from './utils/pg.js';
import { getS3Client, dropFile } from './utils/aws.js';
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
import { convertBigInt } from './utils/utils.js';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

const FRESH_CURSOR_BATCH_SIZE = 250;
let activeFreshQueries = 0;

export function getFDAs(service) {
  return retrieveFDAs(service);
}

export async function getFDA(service, fdaId) {
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

export async function executeQuery({ service, params, fresh = false }) {
  if (fresh) {
    return executeFreshQuery({ service, params });
  }

  const { fdaId, daId, ...rest } = params;

  const conn = await getDBConnection();

  try {
    return await runPreparedStatement(conn, service, fdaId, daId, rest);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function executeQueryStream({
  service,
  params,
  req,
  res,
  fresh = false,
}) {
  if (fresh) {
    return executeFreshQueryStream({ service, params, req, res });
  }

  const { fdaId, daId, ...rest } = params;

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

async function executeFreshQuery({ service, params }) {
  assertFreshQueriesEnabled();

  const releaseFreshSlot = acquireFreshQuerySlot();
  try {
    const { text, values } = await buildFreshQueryStatement(service, params);

    const pgClient = getPgClient(
      config.pg.usr,
      config.pg.pass,
      config.pg.host,
      config.pg.port,
      service,
    );

    await pgClient.connect();

    try {
      const result = await pgClient.query(text, values);
      return convertBigInt(result.rows);
    } finally {
      await pgClient.end();
    }
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
    releaseFreshSlot();
  }
}

async function executeFreshQueryStream({ service, params, req, res }) {
  assertFreshQueriesEnabled();

  const releaseFreshSlot = acquireFreshQuerySlot();
  let pgClient;
  let cursor;
  let cleaned = false;

  const cleanup = async () => {
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
      if (pgClient) {
        await pgClient.end().catch(() => {});
      }
    }
  };

  try {
    const { text, values } = await buildFreshQueryStatement(service, params);

    pgClient = getPgClient(
      config.pg.usr,
      config.pg.pass,
      config.pg.host,
      config.pg.port,
      service,
    );

    req.on('close', () => {
      cleanup().catch(() => {});
    });

    await pgClient.connect();

    cursor = pgClient.query(new Cursor(text, values));
    const readCursor = promisify(cursor.read.bind(cursor));

    res.setHeader('Content-Type', 'application/x-ndjson');

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const rows = await readCursor(FRESH_CURSOR_BATCH_SIZE);
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

    throw new FDAError(
      500,
      'PostgresServerError',
      `Error streaming fresh query: ${e.message}`,
    );
  } finally {
    await cleanup();
    releaseFreshSlot();
  }

  return res.end();
}

function assertFreshQueriesEnabled() {
  if (!config.roles.syncQueries) {
    throw new FDAError(
      503,
      'SyncQueriesDisabled',
      'Fresh query mode is disabled in this instance',
    );
  }
}

function acquireFreshQuerySlot() {
  const parsedMax = Number(config.freshQueries.maxConcurrent);
  const maxFreshQueries = Number.isFinite(parsedMax)
    ? Math.max(1, parsedMax)
    : 5;

  if (activeFreshQueries >= maxFreshQueries) {
    throw new FDAError(
      429,
      'TooManyFreshQueries',
      `Too many concurrent fresh queries (limit ${maxFreshQueries})`,
    );
  }

  activeFreshQueries += 1;

  let released = false;
  return () => {
    if (released) {
      return;
    }

    released = true;
    activeFreshQueries = Math.max(0, activeFreshQueries - 1);
  };
}

async function buildFreshQueryStatement(service, params) {
  const { fdaId, daId, ...rest } = params;

  const da = await retrieveDA(service, fdaId, daId);
  if (!da?.query) {
    throw new FDAError(
      404,
      'DaNotFound',
      `DA ${daId} does not exist in FDA ${fdaId} with service ${service}.`,
    );
  }

  const fda = await retrieveFDA(service, fdaId);
  if (!fda?.query) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} not found in service ${service}`,
    );
  }

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
) {
  const conn = await getDBConnection();

  try {
    const existing = await retrieveDA(service, fdaId, daId);

    if (existing) {
      throw new FDAError(
        409,
        'DuplicatedKey',
        `DA ${daId} already exists in FDA ${fdaId}`,
      );
    }

    checkParams(params);
    // Call to buildDAQuery to detect undesired FROM clausules in query
    buildDAQuery(service, fdaId, userQuery);
    await storeDA(service, fdaId, daId, description, userQuery, params);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function fetchFDA(
  fdaId,
  query,
  service,
  servicePath,
  description,
  refreshPolicy,
) {
  await createFDAMongo(
    fdaId,
    query,
    service,
    servicePath,
    description,
    refreshPolicy,
  );

  const agenda = getAgenda();

  // Execute first fetch immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query,
    service,
  });

  // Schedule refreshes according to policy
  if (refreshPolicy?.type === 'interval' || refreshPolicy?.type === 'cron') {
    // unique is not really needed since we check existence before, but it adds an extra layer of safety in case of duplicate calls
    await agenda.every(
      refreshPolicy.value,
      'refresh-fda',
      { fdaId, query, service },
      {
        unique: {
          name: 'refresh-fda',
          'data.fdaId': fdaId,
        },
      },
    );
  }
}

export async function updateFDA(service, fdaId) {
  const previous = await regenerateFDA(service, fdaId);

  const agenda = getAgenda();

  // Execute refresh immediately (when a fetcher is free)
  await agenda.now('refresh-fda', {
    fdaId,
    query: previous.query,
    service,
  });
}

export async function processFDAAsync(fdaId, query, service) {
  try {
    await updateFDAStatus(service, fdaId, 'fetching', 10);

    await uploadTableToObjStg(service, service, query, service, fdaId);

    await updateFDAStatus(service, fdaId, 'completed', 100);
  } catch (err) {
    await updateFDAStatus(service, fdaId, 'failed', 0, err.message);
    throw err;
  }
}

export async function deleteFDA(service, fdaId) {
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
  await dropFile(s3Client, service, getPath('', fdaId, '.parquet'));
  await removeFDA(service, fdaId);

  const agenda = getAgenda();
  await agenda.cancel({
    name: 'refresh-fda',
    'data.fdaId': fdaId,
  });
}

export function getDAs(service, fdaId) {
  return retrieveDAs(service, fdaId);
}

export async function getDA(service, fdaId, daId) {
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
) {
  const conn = await getDBConnection();

  try {
    checkParams(params);
    // Call to buildDAQuery to detect undesired FROM clausules in query
    buildDAQuery(service, fdaId, userQuery);
    await updateDA(service, fdaId, daId, description, userQuery, params);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function deleteDA(service, fdaId, daId) {
  await removeDA(service, fdaId, daId);
}

//const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function uploadTableToObjStg(service, database, query, bucket, path) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  //await sleep(20000);
  await updateFDAStatus(service, path, 'fetching', 20);
  await uploadTable(s3Client, bucket, database, query, path);

  const conn = await getDBConnection();
  try {
    //await sleep(2000);
    await updateFDAStatus(service, path, 'transforming', 60);
    const parquetPath = getPath(bucket, path, '.parquet');
    await toParquet(conn, getPath(bucket, path, '.csv'), parquetPath);
    //await sleep(2000);
    await updateFDAStatus(service, path, 'uploading', 80);
    //await sleep(2000);
    await dropFile(s3Client, bucket, `${path}.csv`);
  } finally {
    await releaseDBConnection(conn);
  }
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};
