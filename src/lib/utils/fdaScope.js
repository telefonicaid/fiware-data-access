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

export function normalizeScopedServicePath(servicePath) {
  if (!servicePath || typeof servicePath !== 'string') {
    throw new Error('servicePath is required');
  }

  const normalizedServicePath = servicePath.trim();
  if (!normalizedServicePath) {
    throw new Error('servicePath is required');
  }

  if (!/^\/[^/\s]+(\/[^/\s]+)*$/.test(normalizedServicePath)) {
    throw new Error(
      'servicePath must be a non-root absolute path (e.g. /servicepath)',
    );
  }

  return normalizedServicePath;
}

export function getFDAStoragePath(fdaId, servicePath) {
  const normalizedServicePath = normalizeScopedServicePath(servicePath);
  const servicePathScope = normalizedServicePath.replace(/^\//, '');
  return `${servicePathScope}/${fdaId}`;
}

export function buildFDAJobFilter(name, service, fdaId, servicePath) {
  return {
    name,
    'data.service': service,
    'data.fdaId': fdaId,
    'data.servicePath': servicePath,
  };
}
