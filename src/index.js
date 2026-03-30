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
  executeQueryStream,
  assertFDAAccess,
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
import { VALID_VISIBILITIES } from './lib/fda.js';
import {
  validateAllowedFieldsBody,
  parseBooleanQueryParam,
} from './lib/utils/utils.js';
import {
  VALID_OUTPUT_TYPES,
  DEFAULT_OUTPUT_TYPE,
  rowsToCsv,
  rowsToXlsx,
} from './lib/utils/outputFormat.js';

export const app = express();
const PORT = config.port;
const logger = getBasicLogger();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

function parseVisibility(visibility) {
  if (!VALID_VISIBILITIES.includes(visibility)) {
    return null;
  }

  return visibility;
}

function parseServicePath(servicePath) {
  if (!servicePath || typeof servicePath !== 'string') {
    return null;
  }

  const normalizedServicePath = servicePath.trim();

  if (!/^\/(?:[^/\s]+(?:\/[^/\s]+)*)?$/.test(normalizedServicePath)) {
    return null;
  }

  return normalizedServicePath;
}

function parseVisibilityAndServicePath(req) {
  const visibility = parseVisibility(req.params.visibility);
  const servicePath = parseServicePath(req.get('Fiware-ServicePath'));

  if (!visibility) {
    return { error: 'Visibility must be public or private' };
  }

  if (!servicePath) {
    return {
      error:
        'Fiware-ServicePath header is required and must be a valid absolute path (e.g. / or /servicepath)',
    };
  }

  return { visibility, servicePath };
}

app.use((req, res, next) => {
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

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'UP',
    timestamp: new Date().toISOString(),
  });
});

app.get('/:visibility/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  if (!service || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  const fdas = await getFDAs(service);
  const filteredFDAs = fdas.filter(
    (fda) =>
      fda.visibility === parsed.visibility &&
      fda.servicePath === parsed.servicePath,
  );

  return res.status(200).json(filteredFDAs);
});

app.post('/:visibility/fdas', async (req, res) => {
  validateAllowedFieldsBody(req.body, [
    'id',
    'query',
    'description',
    'refreshPolicy',
    'timeColumn',
    'objStgConf',
  ]);
  const { id, query, description, refreshPolicy, timeColumn, objStgConf } =
    req.body;
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  if (!id || !query || !service || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  const finalRefreshPolicy = refreshPolicy ?? { type: 'none' };
  const finalObjStgConf = objStgConf ?? {};

  await fetchFDA(
    id,
    query,
    service,
    parsed.visibility,
    parsed.servicePath,
    description,
    finalRefreshPolicy,
    timeColumn,
    finalObjStgConf,
  );

  return res.status(202).json({
    id,
    status: 'pending',
  });
});

app.get('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;
  const parsed = parseVisibilityAndServicePath(req);

  if (!fdaId || !service || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  const fda = await getFDA(service, fdaId);
  return res.status(200).json(fda);
});

app.put('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;
  const parsed = parseVisibilityAndServicePath(req);

  if (!service || !fdaId || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  if (req.body && Object.keys(req.body).length > 0) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'PUT /fdas does not accept a request body',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  await updateFDA(service, fdaId);

  return res.status(202).json({
    id: fdaId,
    status: 'pending',
  });
});

app.delete('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;
  const parsed = parseVisibilityAndServicePath(req);

  if (!service || !fdaId || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  await deleteFDA(service, fdaId);
  return res.sendStatus(204);
});

app.get('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;
  const parsed = parseVisibilityAndServicePath(req);

  if (!fdaId || !service || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  const das = await getDAs(service, fdaId);
  return res.status(200).json(das);
});

app.post('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const { fdaId } = req.params;

  validateAllowedFieldsBody(req.body, ['id', 'query', 'description', 'params']);
  const { id, description, query, params } = req.body;
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  if (!fdaId || !id || !query || !service || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  await createDA(service, fdaId, id, description, query, params);
  return res.sendStatus(201);
});

app.get('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  if (!service || !fdaId || !daId || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  const da = await getDA(service, fdaId, daId);
  return res.status(200).json(da);
});

app.put('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  validateAllowedFieldsBody(req.body, ['query', 'description', 'params']);
  const { description, query, params } = req.body;

  if (!service || !fdaId || !daId || !query || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  await putDA(service, fdaId, daId, description, query, params);

  return res.sendStatus(204);
});

app.delete('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const parsed = parseVisibilityAndServicePath(req);

  if (!service || !fdaId || !daId || parsed.error) {
    return res.status(400).json({
      error: 'BadRequest',
      description: parsed.error || 'Missing params in the request',
    });
  }

  await assertFDAAccess({
    service,
    fdaId,
    visibility: parsed.visibility,
    servicePath: parsed.servicePath,
  });

  await deleteDA(service, fdaId, daId);
  return res.sendStatus(204);
});

app.get('/:visibility/fdas/:fdaId/das/:daId/data', async (req, res) => {
  const { visibility, fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const servicePath = parseServicePath(req.get('Fiware-ServicePath'));
  const accept = req.get('Accept') || 'application/json';
  const fresh = parseBooleanQueryParam(req.query.fresh, 'fresh');
  const rawOutputType = req.query.outputType || DEFAULT_OUTPUT_TYPE;
  const parsedVisibility = parseVisibility(visibility);

  if (!parsedVisibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Visibility must be public or private',
    });
  }

  if (!servicePath) {
    return res.status(400).json({
      error: 'BadRequest',
      description:
        'Fiware-ServicePath header is required and must be a valid absolute path (e.g. / or /org/site)',
    });
  }

  if (!VALID_OUTPUT_TYPES.includes(rawOutputType)) {
    return res.status(400).json({
      error: 'BadRequest',
      description: `Invalid outputType '${rawOutputType}'. Allowed values: ${VALID_OUTPUT_TYPES.join(', ')}`,
    });
  }

  const queryParams = { ...req.query };
  delete queryParams.fresh;
  delete queryParams.outputType;

  if (!fdaId || !daId || !service) {
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

  // Content negotiation: NDJSON streaming takes precedence over outputType
  if (accept.includes('application/x-ndjson')) {
    return executeQueryStream({
      service,
      visibility: parsedVisibility,
      servicePath,
      params,
      req,
      res,
      fresh,
    });
  }

  const rows = await executeQuery({
    service,
    visibility: parsedVisibility,
    servicePath,
    params,
    fresh,
  });

  if (rawOutputType === 'csv') {
    const csv = rowsToCsv(rows);
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="results.csv"');
    return res.send(csv);
  }

  if (rawOutputType === 'xls') {
    const buffer = await rowsToXlsx(rows);
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    res.setHeader('Content-Disposition', 'attachment; filename="results.xlsx"');
    return res.send(Buffer.from(buffer));
  }

  return res.json(rows);
});

app.post('/plugin/cda/api/doQuery', async (req, res) => {
  const startTime = Date.now();

  const { path, dataAccessId, ...rest } = req.body;
  if (!path || !dataAccessId) {
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

    if (rawOutputType === 'csv') {
      const csv = rowsToCsv(result);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader(
        'Content-Disposition',
        'attachment; filename="results.csv"',
      );
      return res.send(csv);
    }

    if (rawOutputType === 'xls') {
      const buffer = await rowsToXlsx(result);
      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      res.setHeader(
        'Content-Disposition',
        'attachment; filename="results.xlsx"',
      );
      return res.send(Buffer.from(buffer));
    }

    return res.json(result);
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
