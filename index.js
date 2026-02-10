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

import {
  getFDAs,
  fetchFDA,
  query,
  createDA,
  getFDA,
  updateFDA,
  deleteFDA,
  getDAs,
  getDA,
  putDA,
  deleteDA,
} from './lib/fda.js';
import { createIndex, disconnectClient } from './lib/mongo.js';
import { disconnectConnection } from './lib/db.js';
import { destroyS3Client } from './lib/aws.js';
import { config } from './lib/fdaConfig.js';
import {
  initLogger,
  getBasicLogger,
  getInitialLogger,
} from './lib/utils/logger.js';

export const app = express();
const PORT = config.port;
const logger = getBasicLogger();

app.use(express.json());

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
      'API request completed'
    );
  });

  next();
});

app.get('/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');

  if (!service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fdas = await getFDAs(service);
  return res.status(200).json(fdas);
});

app.post('/fdas', async (req, res) => {
  const { id, query, description } = req.body;
  const service = req.get('Fiware-Service');

  if (!id || !query || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await fetchFDA(id, query, service, description);
  return res.sendStatus(201);
});

app.get('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!fdaId || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fda = await getFDA(service, fdaId);
  return res.status(200).json(fda);
});

app.put('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!service || !fdaId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await updateFDA(service, fdaId);
  return res.sendStatus(204);
});

app.delete('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!service || !fdaId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteFDA(service, fdaId);
  return res.sendStatus(204);
});

app.get('/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!fdaId || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const das = await getDAs(service, fdaId);
  return res.status(200).json(das);
});

app.post('/fdas/:fdaId/das', async (req, res) => {
  const { fdaId } = req.params;
  const { id, description, query } = req.body;
  const service = req.get('Fiware-Service');

  if (!fdaId || !id || !description || !query || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await createDA(service, fdaId, id, description, query);
  return res.sendStatus(201);
});

app.get('/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');

  if (!service || !fdaId || !daId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const da = await getDA(service, fdaId, daId);
  return res.status(200).json(da);
});

app.put('/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const { id, description, query } = req.body;

  if (!service || !fdaId || !daId || !id || !description || !query) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await putDA(service, fdaId, daId, id, description, query);
  return res.sendStatus(204);
});

app.delete('/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');

  if (!service || !fdaId || !daId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteDA(service, fdaId, daId);
  return res.sendStatus(204);
});

app.get('/query', async (req, res) => {
  const { fdaId, daId } = req.query;
  const service = req.get('Fiware-Service');

  if (Object.keys(req.query).length === 0 || !fdaId || !daId || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const result = await query(service, req.query);
  return res.json(result);
});

app.get('/doQuery', async (req, res) => {
  const { path, dataAccessId, ...rest } = req.query;
  const service = req.get('Fiware-Service');

  if (
    Object.keys(req.query).length === 0 ||
    !path ||
    !dataAccessId ||
    !service
  ) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const updatedParams = {
    ...rest,
    fdaId: path.split('/').pop(),
    daId: dataAccessId,
  };
  const result = await query(service, updatedParams);
  return res.json(result);
});

if (process.env.NODE_ENV !== 'test') {
  app.listen(PORT, () => {
    logger.debug(`Server listening at port ${PORT}`);
  });

  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    const status = err.statusCode || 500;
    if (status < 500) {
      logger.warn(err);
    } else {
      logger.error(err);
    }

    return res.status(status).json({
      error: err.code || 'InternalServerError',
      description: err.message,
    });
  });

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  startup().catch((err) => {
    logger.debug(`Startup failed:  ${err}`);
    process.exit(1);
  });
}

async function startup() {
  await createIndex();
  initLogger(config);
  getInitialLogger(config).fatal('[INIT]: Initializing app');
}

async function shutdown() {
  await disconnectClient();
  await disconnectConnection();
  await destroyS3Client();
  process.exit(0);
}
