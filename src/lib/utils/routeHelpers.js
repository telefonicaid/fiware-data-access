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

import { FDAError } from '../fdaError.js';
import { DEFAULT_OUTPUT_TYPE, rowsToCsv, rowsToXlsx } from './outputFormat.js';

// Supported MIME types for /data, listed in server-default preference order.
export const DATA_CONTENT_TYPES = [
  'application/json',
  'application/x-ndjson',
  'text/csv',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-excel',
  'application/vnd.fiware.cda+json',
];

const DATA_ACCEPT_CONTENT_TYPE_TO_OUTPUT = {
  'application/json': 'json',
  'application/x-ndjson': 'ndjson',
  'text/csv': 'csv',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xls',
  'application/vnd.ms-excel': 'xls',
  'application/vnd.fiware.cda+json': 'cda',
};

export const QUERY_STYLE_OUTPUT_TYPES = ['json', 'ndjson', 'csv', 'xls', 'cda'];
const VALIDATION_MODES = ['strict', 'unchecked'];
const FDA_ID_PATTERN = /^[a-zA-Z0-9_-]+$/;

export function parseValidationMode(value) {
  if (value === undefined) {
    return 'strict';
  }

  if (typeof value !== 'string') {
    throw new FDAError(
      400,
      'BadRequest',
      'Body field "validationMode" must be a string.',
    );
  }

  const normalized = value.trim().toLowerCase();
  if (!VALIDATION_MODES.includes(normalized)) {
    throw new FDAError(
      400,
      'BadRequest',
      `Body field "validationMode" must be one of: ${VALIDATION_MODES.join(', ')}.`,
    );
  }

  return normalized;
}

export function validateFdaId(id) {
  if (typeof id !== 'string' || id.length === 0 || !FDA_ID_PATTERN.test(id)) {
    throw new FDAError(
      400,
      'InvalidParam',
      'FDA id must contain only alphanumeric characters, hyphens, and underscores.',
    );
  }
}

function throwRequestStyleConflictIfMixed(hasHeaderContext, hasQueryContext) {
  if (hasHeaderContext && hasQueryContext) {
    throw new FDAError(
      409,
      'RequestStyleConflict',
      'Cannot mix query-style and header-style request parameters',
    );
  }
}

export function getOutputTypeFromAcceptHeader(req) {
  const matched = req.accepts(DATA_CONTENT_TYPES);
  if (!matched) {
    throw new FDAError(
      406,
      'NotAcceptable',
      'Accept header must allow application/json, application/x-ndjson, text/csv, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, or application/vnd.fiware.cda+json',
    );
  }

  return DATA_ACCEPT_CONTENT_TYPE_TO_OUTPUT[matched];
}

export function getQueryStyleOutputType(query) {
  const rawOutputType = query.outputType ?? DEFAULT_OUTPUT_TYPE;

  if (!QUERY_STYLE_OUTPUT_TYPES.includes(rawOutputType)) {
    throw new FDAError(
      400,
      'BadRequest',
      `Invalid outputType '${rawOutputType}'. Allowed values: ${QUERY_STYLE_OUTPUT_TYPES.join(', ')}`,
    );
  }

  return rawOutputType;
}

export function splitDaQueryStyleParams(query) {
  const queryParams = { ...query };

  delete queryParams.service;
  delete queryParams.servicePath;
  delete queryParams.outputType;

  return queryParams;
}

export function resolveServiceContextFromRequest(req) {
  const headerService = req.get('Fiware-Service');
  const headerServicePath = req.get('Fiware-ServicePath');
  const queryService = req.query.service;
  const queryServicePath = req.query.servicePath;

  const hasHeaderContext =
    headerService !== undefined || headerServicePath !== undefined;
  const hasQueryContext =
    queryService !== undefined || queryServicePath !== undefined;

  throwRequestStyleConflictIfMixed(hasHeaderContext, hasQueryContext);

  if (hasQueryContext) {
    if (!queryService || !queryServicePath) {
      throw new FDAError(400, 'BadRequest', 'Missing params in the request');
    }

    return {
      style: 'query',
      service: queryService,
      servicePath: queryServicePath,
    };
  }

  if (!headerService || !headerServicePath) {
    throw new FDAError(400, 'BadRequest', 'Missing params in the request');
  }

  return {
    style: 'header',
    service: headerService,
    servicePath: headerServicePath,
  };
}

export async function sendRowsByOutputType(res, rows, outputType) {
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
