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

import express from 'express';

import { startFetcher } from './fetcher.js';
import { shutdownAgenda, initAgenda } from './lib/jobs.js';
import {
  getFDAs,
  fetchFDA,
  executeQuery,
  executeFDAQuery,
  executeQueryStream,
  executeFDAQueryStream,
  createDA,
  getFDA,
  updateFDA,
  deleteFDA,
  getDAs,
  getDA,
  putDA,
  deleteDA,
} from './lib/fda.js';
import { createIndex, disconnectClient } from './lib/utils/mongo.js';
import { destroyS3Client } from './lib/utils/aws.js';
import { closePgPools } from './lib/utils/pg.js';
import { config } from './lib/fdaConfig.js';
import {
  initLogger,
  getBasicLogger,
  getInitialLogger,
} from './lib/utils/logger.js';
import { handleCdaQuery } from './lib/compat/cdaAdapter.js';
import {
  validateAllowedFieldsBody,
  validateForbiddenFieldsQuery,
  parseBooleanQueryParam,
} from './lib/utils/utils.js';
import {
  VALID_OUTPUT_TYPES,
  DEFAULT_OUTPUT_TYPE,
  rowsToCsv,
  rowsToXlsx,
} from './lib/utils/outputFormat.js';
import {
  onRequestStart,
  onRequestFinish,
  buildHealthPayload,
  buildMetricsText,
  getMetricsContentType,
} from './lib/metrics.js';

export const app = express();
const PORT = config.port;
const logger = getBasicLogger();

// Supported MIME types for /data, listed in server-default preference order.
const DATA_CONTENT_TYPES = [
  'application/json',
  'application/x-ndjson',
  'text/csv',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-excel',
];

const DATA_ACCEPT_CONTENT_TYPE_TO_OUTPUT = {
  'application/json': 'json',
  'application/x-ndjson': 'ndjson',
  'text/csv': 'csv',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xls',
  'application/vnd.ms-excel': 'xls',
};

async function sendRowsByOutputType(res, rows, outputType) {
  if (outputType === 'csv') {
    const csv = rowsToCsv(rows);
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="results.csv"');
    return res.send(csv);
  }

  if (outputType === 'xls') {
    const buffer = await rowsToXlsx(rows);
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    res.setHeader('Content-Disposition', 'attachment; filename="results.xlsx"');
    return res.send(Buffer.from(buffer));
  }

  return res.json(rows);
}

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  const reqStartMs = onRequestStart();
  const oldSend = res.send;
  const oldJson = res.json;

  function capture(body) {
    try {
      let str = typeof body === 'string' ? body : JSON.stringify(body);

      if (str.length > config.logger.resSize) {
        str = str.slice(0, config.logger.resSize) + '…[truncated]';
      }

      res.locals.responseBody = str;
    } catch {
      res.locals.responseBody = '[unserializable body]';
    }
  }

  res.send = function (body) {
    capture(body);
    return oldSend.call(this, body);
  };

  res.json = function (body) {
    capture(body);
    return oldJson.call(this, body);
  };

  const start = Date.now();

  res.on('finish', () => {
    onRequestFinish(req, res, reqStartMs);

    logger.info(
      {
        method: req.method,
        path: req.originalUrl,
        fiwareService: req.get('Fiware-Service'),
        reqParams: `${JSON.stringify(req.params)}`,
        reqQuery: `${JSON.stringify(req.query)}`,
        reqBody: `${JSON.stringify(req.body)}`,
        resCode: res.statusCode,
        resMsg: res.statusMessage,
        durationMs: Date.now() - start,
        ip: req.ip,
        userAgent: req.headers['user-agent'],
        resSize: res.getHeader('Content-Length'),
        resBody: res.locals.responseBody,
      },
      'API request completed',
    );
  });

  next();
});

app.get('/health', async (req, res) => {
  const payload = await buildHealthPayload();
  res.status(200).json(payload);
});

app.get('/metrics', async (req, res) => {
  const contentNegotiation = getMetricsContentType(req.get('Accept'));

  if (!contentNegotiation.ok) {
    return res.status(406).json({
      error: 'NotAcceptable',
      description:
        'Accept header must allow application/openmetrics-text or text/plain',
    });
  }

  res.setHeader('Content-Type', contentNegotiation.contentType);
  const payload = await buildMetricsText();
  return res.status(200).send(payload);
});

app.get('/:visibility/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility } = req.params;

  if (!service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fdas = await getFDAs(service, visibility, servicePath);
  return res.status(200).json(fdas);
});

app.post('/:visibility/fdas', async (req, res) => {
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, [
    'id',
    'query',
    'description',
    'refreshPolicy',
    'timeColumn',
    'objStgConf',
    'cached',
  ]);
  const {
    id,
    query,
    description,
    refreshPolicy,
    timeColumn,
    objStgConf,
    cached,
  } = body;
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility } = req.params;
  const defaultDataAccessByConfig = config.defaultDataAccess?.enabled ?? true;
  const defaultDataAccessEnabled =
    req.query.defaultDataAccess === undefined
      ? defaultDataAccessByConfig
      : parseBooleanQueryParam(
          req.query.defaultDataAccess,
          'defaultDataAccess',
          true,
        );
  const cachedEnabled =
    cached === undefined
      ? true
      : parseBooleanQueryParam(cached, 'cached', true);

  if (!id || !query || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const finalRefreshPolicy = refreshPolicy ?? { type: 'none' };
  const finalObjStgConf = objStgConf ?? {};

  await fetchFDA(
    id,
    query,
    service,
    visibility,
    servicePath,
    description,
    finalRefreshPolicy,
    timeColumn,
    finalObjStgConf,
    defaultDataAccessEnabled,
    cachedEnabled,
  );

  return res.status(202).json({
    id,
    status: 'pending',
  });
});

app.get('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fda = await getFDA(service, fdaId, visibility, servicePath);
  return res.status(200).json(fda);
});

app.put('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!service || !fdaId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  if (req.body && Object.keys(req.body).length > 0) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'PUT /fdas does not accept a request body',
    });
  }

  await updateFDA(service, fdaId, visibility, servicePath);

  return res.status(202).json({
    id: fdaId,
    status: 'pending',
  });
});

app.delete('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!service || !fdaId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteFDA(service, fdaId, visibility, servicePath);
  return res.sendStatus(204);
});

app.get('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const das = await getDAs(service, fdaId, visibility, servicePath);
  return res.status(200).json(das);
});

app.post('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['id', 'query', 'description', 'params']);
  const { id, description, query, params } = body;

  if (!fdaId || !id || !query || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await createDA(
    service,
    fdaId,
    id,
    description,
    query,
    params,
    visibility,
    servicePath,
  );
  return res.sendStatus(201);
});

app.get('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;

  if (!service || !fdaId || !daId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const da = await getDA(service, fdaId, daId, visibility, servicePath);
  return res.status(200).json(da);
});

app.put('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['query', 'description', 'params']);
  const { description, query, params } = body;

  if (!service || !fdaId || !daId || !query || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await putDA(
    service,
    fdaId,
    daId,
    description,
    query,
    params,
    visibility,
    servicePath,
  );

  return res.sendStatus(204);
});

app.delete('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;

  if (!service || !fdaId || !daId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteDA(service, fdaId, daId, visibility, servicePath);
  return res.sendStatus(204);
});

app.get('/:visibility/fdas/:fdaId/das/:daId/data', async (req, res) => {
  const { visibility, fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');

  validateForbiddenFieldsQuery(req.query, ['outputType', 'fresh']);

  const matched = req.accepts(DATA_CONTENT_TYPES);
  if (!matched) {
    return res.status(406).json({
      error: 'NotAcceptable',
      description:
        'Accept header must allow application/json, application/x-ndjson, text/csv, or application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
  }

  const outputType = DATA_ACCEPT_CONTENT_TYPE_TO_OUTPUT[matched];

  const queryParams = { ...req.query };
  delete queryParams.outputType;

  if (!fdaId || !daId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const params = {
    fdaId,
    daId,
    ...queryParams,
  };

  if (outputType === 'ndjson' || outputType === 'csv') {
    return executeQueryStream({
      service,
      visibility,
      servicePath,
      params,
      req,
      res,
      format: outputType,
    });
  }

  const rows = await executeQuery({
    service,
    visibility,
    servicePath,
    params,
  });

  return sendRowsByOutputType(res, rows, outputType);
});

app.get('/:visibility/fdas/:fdaId/data', async (req, res) => {
  const { visibility, fdaId } = req.params;
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');

  if (Object.keys(req.query).length > 0) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'FDA fresh query does not accept query parameters',
    });
  }

  const matched = req.accepts(DATA_CONTENT_TYPES);
  if (!matched) {
    return res.status(406).json({
      error: 'NotAcceptable',
      description:
        'Accept header must allow application/json, application/x-ndjson, text/csv, or application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
  }

  const outputType = DATA_ACCEPT_CONTENT_TYPE_TO_OUTPUT[matched];

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  if (outputType === 'ndjson' || outputType === 'csv') {
    return executeFDAQueryStream({
      service,
      visibility,
      servicePath,
      fdaId,
      req,
      res,
      format: outputType,
    });
  }

  const rows = await executeFDAQuery({
    service,
    visibility,
    servicePath,
    fdaId,
  });

  return sendRowsByOutputType(res, rows, outputType);
});

app.post('/plugin/cda/api/doQuery', async (req, res) => {
  const startTime = Date.now();

  const requestId = Math.random().toString(36).substring(7);

  logger.info('[CDA REQUEST]', {
    requestId,
    method: req.method,
    url: req.originalUrl,
    headers: {
      contentType: req.get('Content-Type'),
      accept: req.get('Accept'),
      fiwareService: req.get('Fiware-Service'),
    },
    query: req.query,
    bodyKeys: Object.keys(req.body || {}),
    paramKeys: Object.keys(req.body || {}).filter((k) => k.startsWith('param')),
    timestamp: new Date().toISOString(),
  });

  logger.debug('[CDA REQUEST BODY STRINGIFIED]', {
    requestId,
    body: JSON.stringify(req.body),
  });

  const { path, dataAccessId } = req.body;

  if (!path || !dataAccessId) {
    logger.warn('[CDA BAD REQUEST]', {
      requestId,
      reason: 'Missing path or dataAccessId',
    });

    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const rawOutputType = req.body.outputType || DEFAULT_OUTPUT_TYPE;

  if (!VALID_OUTPUT_TYPES.includes(rawOutputType)) {
    return res.status(400).json({
      error: 'BadRequest',
      description: `Invalid outputType '${rawOutputType}'. Allowed values: ${VALID_OUTPUT_TYPES.join(', ')}`,
    });
  }

  try {
    const result = await handleCdaQuery({
      body: req.body,
      outputType: rawOutputType,
    });

    logger.info('[CDA RESPONSE]', {
      requestId,
      status: 200,
      durationMs: Date.now() - startTime,
      rows: result?.resultset?.length,
      totalRows: result?.queryInfo?.totalRows,
    });

    logger.debug('[CDA RESPONSE DETAIL]', {
      requestId,
      metadata: result?.metadata,
      queryInfo: result?.queryInfo,
      sampleRows: result?.resultset?.slice(0, 2),
    });

    return sendRowsByOutputType(res, result, rawOutputType);
  } catch (err) {
    logger.error('Error executing query:', err);
    const status = err.status || 500;

    return res.status(status).json({
      error: err.type || 'InternalServerError',
      description: err.message || 'Unexpected error executing query',
    });
  }
});

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  const status = err.status || 500;
  if (status < 500) {
    logger.warn(err);
  } else {
    logger.error(err);
  }

  return res.status(status).json({
    error: err.type || 'InternalServerError',
    description: err.message,
  });
});

if (process.env.NODE_ENV !== 'test') {
  startup()
    .then(() => {
      if (config.roles.apiServer) {
        app.listen(PORT, () => {
          logger.debug(`API Server listening at port ${PORT}`);
        });
      }

      if (config.roles.fetcher) {
        startFetcher().catch((err) => {
          logger.error('[Fetcher] Failed to start', err);
          process.exit(1);
        });
      }
    })
    .catch((err) => {
      logger.error(`Startup failed: ${err}`);
      process.exit(1);
    });

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

async function startup() {
  if (!config.roles.apiServer && !config.roles.fetcher) {
    throw new Error('At least one FDA role must be enabled');
  }

  await createIndex();
  initLogger(config);

  if (config.roles.apiServer || config.roles.fetcher) {
    await initAgenda();
  }

  getInitialLogger(config).fatal('[INIT]: Initializing app');
}

let shuttingDown = false;
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;

  logger.info('[SHUTDOWN] Graceful shutdown started');

  try {
    if (config.roles.fetcher) {
      await shutdownAgenda();
    }

    await disconnectClient();
    await destroyS3Client();
    await closePgPools();

    logger.info('[SHUTDOWN] Completed');
    process.exit(0);
  } catch (err) {
    logger.error('[SHUTDOWN] Failed', err);
    process.exit(1);
  }
}
