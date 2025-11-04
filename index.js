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

import { queryFDA, storeSet } from './lib/poc.js';

const app = express();
const PORT = 8080;

app.use(express.json());

app.listen(PORT, () => {
  console.log(`Server listening at port ${PORT}`);
});

app.post('/queryFDA', async (req, res) => {
  const { data, cda, path } = req.body;
  const { columns, filters } = data;

  if (!data) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    const result = await queryFDA(path, cda, columns, filters);
    res.json(result);
  } catch (err) {
    console.error(' Error in /fda:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/storeSet', async (req, res) => {
  const { fda, filePath, path } = req.body;

  if (!fda || !filePath || !path) {
    return res.status(418).json({ message: 'missing params in body' });
  }

  try {
    await storeSet(path, filePath, fda);
    res.status(201).json({ message: 'Set stored correctly' });
  } catch (err) {
    console.error(' Error in /storeSet:', err);
    res.status(500).json({ error: err.message });
  }
});
