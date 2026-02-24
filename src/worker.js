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

import { initAgenda, shutdownAgenda } from './lib/jobs.js';
import { processFDAAsync } from './lib/fda.js';

async function startWorker() {
  const agenda = await initAgenda();

  agenda.define('refresh-fda', async (job) => {
    const { fdaId, query, service } = job.attrs.data;
    console.log(`[Worker] Hello, ${fdaId}!`);
    await processFDAAsync(fdaId, query, service);
  });

  agenda.on('start', (job) => {
    console.log(`[Worker] Job ${job.attrs.name} starting...`);
  });

  agenda.on('success', (job) => {
    console.log(`[Worker] Job ${job.attrs.name} completed successfully`);
  });

  agenda.on('fail', (err, job) => {
    console.error(`[Worker] Job ${job.attrs.name} failed:`, err);
  });

  await agenda.start();
  console.log('[Worker] Agenda started');
}

startWorker().catch((err) => {
  console.error('[Worker] Failed to start:', err);
  process.exit(1);
});

process.on('SIGINT', async () => {
  await shutdownAgenda();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await shutdownAgenda();
  process.exit(0);
});
