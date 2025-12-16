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
} from './lib/fda.js';
import { createIndex, disconnectClient } from './lib/mongo.js';
import { disconnectConnection } from './lib/db.js';
import { destroyS3Client } from './lib/aws.js';

const app = express();
const PORT = 8080;

app.use(express.json());

app.get('/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');

  if (!service) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const fdas = await getFDAs(service);
    res.status(200).json(fdas);
  } catch (err) {
    console.error(' Error in GET /fdas:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/fdas', async (req, res) => {
  const { id, database, schema, table, bucket, path } = req.body;
  const service = req.get('Fiware-Service');

  if (!id || !database || !schema || !table || !bucket || !path || !service) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    await fetchFDA(id, database, schema, table, bucket, path, service);
    res.sendStatus(201);
  } catch (err) {
    console.error(' Error in POST /fdas:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!fdaId || !service) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const fda = await getFDA(service, fdaId);
    res.status(200).json(fda);
  } catch (err) {
    console.error(`Error in GET /fdas/${fdaId}: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.put('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!service || !fdaId) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    await updateFDA(service, fdaId);
    res.sendStatus(204);
  } catch (err) {
    console.error(`Error in PUT /fdas/${fdaId}: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!service || !fdaId) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const statusCode = await deleteFDA(service, fdaId);
    res.sendStatus(statusCode);
  } catch (err) {
    console.error(`Error in DELETE /fdas/${fdaId}: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.get('/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { fdaId } = req.params;

  if (!fdaId || !service) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const das = await getDAs(service, fdaId);
    res.status(200).json(das);
  } catch (err) {
    console.error(`Error in GET /fdas/${fdaId}/das: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.post('/fdas/:fdaId/das', async (req, res) => {
  const { fdaId } = req.params;
  const { id, description, query } = req.body;
  const service = req.get('Fiware-Service');

  if (!fdaId || !id || !description || !query || !service) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    await createDA(service, fdaId, id, description, query);
    res.sendStatus(201);
  } catch (err) {
    console.error(`Error in POST /fdas/${fdaId}/das: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.get('/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');

  if (!service || !fdaId || !daId) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const da = await getDA(service, fdaId, daId);
    if (da) {
      res.status(200).json(da);
    } else {
      res.sendStatus(404);
    }
  } catch (err) {
    console.error(`Error in GET /fdas/${fdaId}/das/${daId}: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.put('/fdas/:fdaId/das/:daId', async (req, res) => {
  const { fdaId, daId } = req.params;
  const service = req.get('Fiware-Service');
  const { id, description, query } = req.body;

  if (!service || !fdaId || !daId || !id || !description || !query) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    await putDA(service, fdaId, daId, id, description, query);
    res.sendStatus(204);
  } catch (err) {
    console.error(`Error in PUT /fdas/${fdaId}/das/${daId}: ${err}`);
    res.status(500).json({ error: err.message });
  }
});

app.get('/query', async (req, res) => {
  const { fdaId, daId } = req.query;
  const service = req.get('Fiware-Service');

  if (Object.keys(req.query).length === 0 || !fdaId || !daId || !service) {
    return res.status(418).json({ message: 'missing params in request' });
  }

  try {
    const result = await query(service, req.query);
    res.json(result);
  } catch (err) {
    console.error(' Error in /query:', err);
    res.status(500).json({ error: err.message });
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
