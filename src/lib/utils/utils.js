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
import { CronExpressionParser } from 'cron-parser';

let activeFreshQueries = 0;

// Convert BigInt values to numbers for JSON serialization
export function convertBigInt(obj) {
  if (typeof obj === 'bigint') {
    return Number(obj);
  }
  if (Array.isArray(obj)) {
    return obj.map(convertBigInt);
  }
  if (obj !== null && typeof obj === 'object') {
    const converted = {};
    for (const key in obj) {
      converted[key] = convertBigInt(obj[key]);
    }
    return converted;
  }
  return obj;
}

// Validate that the request body only contains allowed fields
export function validateAllowedFieldsBody(body, allowedFields) {
  const keys = Object.keys(body);
  const invalid = keys.filter((k) => !allowedFields.includes(k));
  if (invalid.length > 0) {
    const err = new Error(`Invalid fields in request body, check your request`);
    err.status = 400;
    err.type = 'BadRequest';
    throw err;
  }
}

export function parseBooleanQueryParam(value, name) {
  if (value === undefined) {
    return false;
  }

  if (value === true || value === false) {
    return value;
  }

  if (typeof value !== 'string') {
    throw new FDAError(
      400,
      'BadRequest',
      `Query param "${name}" must be a boolean.`,
    );
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === 'true' || normalized === '1') {
    return true;
  }

  if (normalized === 'false' || normalized === '0') {
    return false;
  }

  throw new FDAError(
    400,
    'BadRequest',
    `Query param "${name}" must be a boolean.`,
  );
}

export function assertFreshQueriesEnabled(isEnabled) {
  if (!isEnabled) {
    throw new FDAError(
      503,
      'SyncQueriesDisabled',
      'Fresh query mode is disabled in this instance',
    );
  }
}

export function acquireFreshQuerySlot(maxConcurrent) {
  const parsedMax = Number(maxConcurrent);
  const maxFreshQueries = Number.isFinite(parsedMax)
    ? Math.max(1, parsedMax)
    : 5;

  if (activeFreshQueries >= maxFreshQueries) {
    throw new FDAError(
      429,
      'TooManyFreshQueries',
      `Too many concurrent fresh queries (limit ${maxFreshQueries})`,
    );
  }

  activeFreshQueries += 1;

  let released = false;
  return () => {
    if (released) {
      return;
    }

    released = true;
    activeFreshQueries = Math.max(0, activeFreshQueries - 1);
  };
}

export function getWindowDate(windowSize) {
  const now = new Date();

  const map = {
    day: () => now.setDate(now.getDate() - 1),
    week: () => now.setDate(now.getDate() - 7),
    month: () => now.setMonth(now.getMonth() - 1),
    year: () => now.setFullYear(now.getFullYear() - 1),
  };

  map[windowSize]?.();
  return map[windowSize] ? now : undefined;
}

export function convertRefreshIntervalToMs(interval) {
  if (!interval || typeof interval !== 'string' || !interval.trim()) {
    return null;
  }

  const normalized = interval.trim().toLowerCase();

  // Parse human-readable Agenda format: "number unit"
  const match = normalized.match(
    /^(\d+)\s*(second|minute|hour|day|week|month|year)s?$/,
  );
  if (match) {
    const [, quantity, unit] = match;
    const num = parseInt(quantity, 10);
    const unitMs = {
      second: 1000,
      minute: 60 * 1000,
      hour: 60 * 60 * 1000,
      day: 24 * 60 * 60 * 1000,
      week: 7 * 24 * 60 * 60 * 1000,
      month: 30 * 24 * 60 * 60 * 1000,
      year: 365 * 24 * 60 * 60 * 1000,
    };
    return num * (unitMs[unit] || 0);
  }

  // Cron intervals
  const cronMs = cronToIntervalMs(interval);
  if (cronMs !== null) {
    return cronMs;
  }

  return null;
}

function cronToIntervalMs(cron) {
  const interval = CronExpressionParser.parse(cron);
  if (!interval) {
    return null;
  }

  // We need to get the difference between two consecutive runs to know the actual interval
  const next = interval.next().getTime();
  const next2 = interval.next().getTime();

  return next2 - next;
}

export function getFinalQuery(query, timeColumn) {
  if (typeof timeColumn !== 'string' || !/^[a-zA-Z0-9_]+$/.test(timeColumn)) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid time column name "${timeColumn}".`,
    );
  }

  const upper = query.toUpperCase();
  const start = upper.indexOf('SELECT');

  if (start === -1) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid query format. Missing SELECT statement.`,
    );
  }

  // Search for the position to insert the time column, which is right after the SELECT keyword and any following whitespace
  let insertPos = start + 6;
  while (query[insertPos] === ' ') {
    insertPos++;
  }

  // find end of SELECT clause (before FROM)
  const fromIndex = upper.indexOf('FROM');
  const selectClause = query.slice(insertPos, fromIndex).trim();
  const columns = selectClause.split(',').map((c) => c.trim());

  // Check if SELECT already has the timecolumn
  if (selectClause.startsWith('*') || columns.includes(timeColumn)) {
    return query;
  }

  return query.slice(0, insertPos) + timeColumn + ', ' + query.slice(insertPos);
}
