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

import { config } from './fdaConfig.js';
import { recordFDAAccesses } from './utils/mongo.js';
import { onDAQuery } from './metrics.js';
import { getBasicLogger } from './utils/logger.js';

const logger = getBasicLogger();
const pendingAccesses = new Map();
let flushTimer;

function ensureFlushTimer() {
  if (flushTimer) {
    return;
  }

  flushTimer = setInterval(() => {
    flushAccesses().catch(() => {});
  }, config.accessTracking.flushIntervalMs);
  flushTimer.unref?.();
}

export function recordAccess({ service, servicePath, fdaId, daId }) {
  const now = new Date();
  const key = JSON.stringify([service, servicePath, fdaId]);
  const entry = pendingAccesses.get(key) ?? {
    service,
    servicePath,
    fdaId,
    count: 0,
    lastAccessAt: now,
    das: {},
  };

  entry.count += 1;
  entry.lastAccessAt = now;

  if (daId) {
    const daEntry = entry.das[daId] ?? { count: 0, lastAccessAt: now };
    daEntry.count += 1;
    daEntry.lastAccessAt = now;
    entry.das[daId] = daEntry;
  }

  pendingAccesses.set(key, entry);
  onDAQuery({ service, servicePath, fdaId });
  ensureFlushTimer();
}

export async function flushAccesses() {
  if (pendingAccesses.size === 0) {
    return;
  }

  const entries = [...pendingAccesses.values()];
  pendingAccesses.clear();

  try {
    await recordFDAAccesses(entries);
  } catch (error) {
    logger.warn(
      { err: error, entries: entries.length },
      'Failed to persist FDA access counters',
    );
  }
}

export async function stopAccessTracker() {
  if (flushTimer) {
    clearInterval(flushTimer);
    flushTimer = undefined;
  }

  await flushAccesses();
}
