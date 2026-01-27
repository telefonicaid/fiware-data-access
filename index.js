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

const app = express();
const PORT = config.port;

let responseCode;
let error;
let resDesc;

export function setResponse(code, errStr = '', errDesc = '') {
  responseCode = code;
  error = errStr;
  resDesc = errDesc;
}

app.use(express.json());

app.get('/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');

  if (!service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  try {
    const fdas = await getFDAs(service);
    return res.status(200).json(fdas);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
});

app.post('/fdas', async (req, res) => {
  const { id, database, query, path, description } = req.body;
  const service = req.get('Fiware-Service');

  if (!id || !database || !query || !path || !service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  try {
    await fetchFDA(id, database, query, path, service, description);
    return res.sendStatus(201);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    const fda = await getFDA(service, fdaId);
    return res.status(200).json(fda);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    await updateFDA(service, fdaId);
    return res.sendStatus(204);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    await deleteFDA(service, fdaId);
    return res.sendStatus(204);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    const das = await getDAs(service, fdaId);
    return res.status(200).json(das);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    await createDA(service, fdaId, id, description, query);
    return res.sendStatus(201);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    const da = await getDA(service, fdaId, daId);
    return res.status(200).json(da);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    await putDA(service, fdaId, daId, id, description, query);
    return res.sendStatus(204);
  } catch (err) {
    console.error(`Error in PUT /fdas/${fdaId}/das/${daId}: ${err}`);
    return res.status(responseCode).json({ error: err.message });
  }
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

  try {
    await deleteDA(service, fdaId, daId);
    return res.sendStatus(204);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    const result = await query(service, req.query);
    return res.json(result);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
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

  try {
    const updatedParams = {
      ...rest,
      fdaId: path.split('/').pop(),
      daId: dataAccessId,
    };
    const result = await query(service, updatedParams);
    return res.json(result);
  } catch (err) {
    return res.status(responseCode).json({ error, description: resDesc });
  }
});

app.listen(PORT, () => {
  console.log(`Server listening at port ${PORT}`);
});

async function startup() {
  await createIndex();
}

async function shutdown() {
  await disconnectClient();
  await disconnectConnection();
  await destroyS3Client();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
startup();
