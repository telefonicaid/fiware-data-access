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

import { Transform } from 'node:stream';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';
import { retrieveQuotas, aggregateFDAUsage } from './utils/mongo.js';

function toLimit(...candidates) {
  for (const candidate of candidates) {
    if (candidate === undefined || candidate === null) {
      continue;
    }

    const value = Number(candidate);
    return Number.isFinite(value) && value > 0 ? value : null;
  }

  return null;
}

function remaining(limit, used) {
  return limit === null ? null : Math.max(0, limit - used);
}

function buildScopeUsage(limits, used) {
  return {
    limits,
    used,
    available: {
      fdas: remaining(limits.maxFDAs, used.fdas),
      bytes: remaining(limits.maxBytes, used.bytes),
    },
  };
}

export async function resolveLimits(service, servicePath) {
  const quotas = await retrieveQuotas(service);
  const serviceQuota = quotas.find((quota) => !quota.servicePath) ?? {};
  const servicePathQuota =
    (servicePath &&
      quotas.find((quota) => quota.servicePath === servicePath)) ||
    {};

  return {
    service: {
      maxFDAs: toLimit(serviceQuota.maxFDAs, config.quotas.maxFDAsPerService),
      maxBytes: toLimit(
        serviceQuota.maxBytes,
        config.quotas.maxBytesPerService,
      ),
    },
    servicePath: {
      maxFDAs: toLimit(
        servicePathQuota.maxFDAs,
        serviceQuota.maxFDAsPerServicePath,
        config.quotas.maxFDAsPerServicePath,
      ),
      maxBytes: toLimit(
        servicePathQuota.maxBytes,
        serviceQuota.maxBytesPerServicePath,
        config.quotas.maxBytesPerServicePath,
      ),
    },
    fda: {
      maxBytes: toLimit(
        servicePathQuota.maxBytesPerFDA,
        serviceQuota.maxBytesPerFDA,
        config.quotas.maxBytesPerFDA,
      ),
      maxFetchBytes: toLimit(
        servicePathQuota.maxFetchBytes,
        serviceQuota.maxFetchBytes,
        config.quotas.maxFetchBytes,
      ),
    },
  };
}

export async function getUsage(service, servicePath) {
  const [limits, byServicePath] = await Promise.all([
    resolveLimits(service, servicePath),
    aggregateFDAUsage(service),
  ]);

  const serviceUsed = byServicePath.reduce(
    (total, group) => ({
      fdas: total.fdas + group.fdas,
      bytes: total.bytes + group.bytes,
    }),
    { fdas: 0, bytes: 0 },
  );

  const usage = {
    service: buildScopeUsage(limits.service, serviceUsed),
    fda: { limits: limits.fda },
  };

  if (servicePath) {
    const group = byServicePath.find(
      (candidate) => candidate.servicePath === servicePath,
    );
    usage.servicePath = buildScopeUsage(limits.servicePath, {
      fdas: group?.fdas ?? 0,
      bytes: group?.bytes ?? 0,
    });
  } else {
    usage.byServicePath = byServicePath;
  }

  return usage;
}

function assertScopeHasRoom(scopeName, scopeValue, { limits, used }) {
  if (limits.maxFDAs !== null && used.fdas >= limits.maxFDAs) {
    throw new FDAError(
      403,
      'QuotaExceeded',
      `${scopeName} ${scopeValue} already holds ${used.fdas} FDAs out of the ${limits.maxFDAs} allowed`,
    );
  }

  if (limits.maxBytes !== null && used.bytes >= limits.maxBytes) {
    throw new FDAError(
      403,
      'QuotaExceeded',
      `${scopeName} ${scopeValue} already stores ${used.bytes} bytes out of the ${limits.maxBytes} allowed`,
    );
  }
}

export async function assertCanCreateFDA(service, servicePath) {
  const usage = await getUsage(service, servicePath);

  assertScopeHasRoom('Service', service, usage.service);
  assertScopeHasRoom('ServicePath', servicePath, usage.servicePath);
}

export function assertFDAWithinStorageLimit(fdaId, storageBytes, maxBytes) {
  if (maxBytes !== null && storageBytes > maxBytes) {
    throw new FDAError(
      403,
      'QuotaExceeded',
      `FDA ${fdaId} stores ${storageBytes} bytes, above the ${maxBytes} allowed per FDA`,
    );
  }
}

export function createFetchByteCounter(maxFetchBytes) {
  let fetchedBytes = 0;

  return (chunk) => {
    fetchedBytes += Buffer.byteLength(chunk);

    if (maxFetchBytes !== null && fetchedBytes > maxFetchBytes) {
      throw new FDAError(
        403,
        'QuotaExceeded',
        `Fetch aborted after exceeding the ${maxFetchBytes} bytes allowed per fetch`,
      );
    }
  };
}

export function createFetchByteGuard(maxFetchBytes) {
  const countBytes = createFetchByteCounter(maxFetchBytes);

  return new Transform({
    transform(chunk, _encoding, callback) {
      try {
        countBytes(chunk);
        callback(null, chunk);
      } catch (error) {
        callback(error);
      }
    },
  });
}
