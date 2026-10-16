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

import { DAY_MS, WEEK_MS, RANGE_MARGIN_MS } from '../constants.js';

function isUnset(value) {
  return value === undefined || value === null || value === '';
}

function toTimestamp(value) {
  if (value instanceof Date) {
    return value.getTime();
  }

  if (typeof value !== 'string') {
    return Number.NaN;
  }

  return new Date(value.trim()).getTime();
}

export function resolveTimeRange(timeRange, params = {}) {
  if (!timeRange || typeof timeRange !== 'object') {
    return null;
  }

  const startValue = timeRange.start ? params?.[timeRange.start] : undefined;
  const finishValue = timeRange.finish ? params?.[timeRange.finish] : undefined;

  if (isUnset(startValue) && isUnset(finishValue)) {
    return null;
  }

  let from = null;
  let to = null;

  if (!isUnset(startValue)) {
    const timestamp = toTimestamp(startValue);
    if (Number.isNaN(timestamp)) {
      return null;
    }
    from = new Date(timestamp - RANGE_MARGIN_MS);
  }

  if (!isUnset(finishValue)) {
    const timestamp = toTimestamp(finishValue);
    if (Number.isNaN(timestamp)) {
      return null;
    }
    to = new Date(timestamp + RANGE_MARGIN_MS);
  }

  return { from, to };
}

function getWeekInterval(year, week) {
  const firstDayOfYear = Date.UTC(year, 0, 1);
  const firstDayOfNextYear = Date.UTC(year + 1, 0, 1);
  const dayOfWeek = new Date(firstDayOfYear).getUTCDay();
  const firstMonday = firstDayOfYear + ((8 - dayOfWeek) % 7) * DAY_MS;
  const start = firstMonday + (week - 1) * WEEK_MS;

  return {
    start: new Date(Math.max(start, firstDayOfYear)),
    end: new Date(Math.min(start + WEEK_MS, firstDayOfNextYear)),
  };
}

export function getPartitionInterval(key) {
  if (typeof key !== 'string') {
    return null;
  }

  const yearMatch = /(?:^|\/)year=(\d{4})(?:\/|$)/.exec(key);
  if (!yearMatch) {
    return null;
  }
  const year = Number(yearMatch[1]);

  const weekMatch = /(?:^|\/)week=(\d{4})-(\d{1,2})(?:\/|$)/.exec(key);
  if (weekMatch) {
    return getWeekInterval(Number(weekMatch[1]), Number(weekMatch[2]));
  }

  const monthMatch = /(?:^|\/)month=(\d{1,2})(?:\/|$)/.exec(key);
  if (!monthMatch) {
    return {
      start: new Date(Date.UTC(year, 0, 1)),
      end: new Date(Date.UTC(year + 1, 0, 1)),
    };
  }
  const month = Number(monthMatch[1]);

  const dayMatch = /(?:^|\/)day=(\d{1,2})(?:\/|$)/.exec(key);
  if (!dayMatch) {
    return {
      start: new Date(Date.UTC(year, month - 1, 1)),
      end: new Date(Date.UTC(year, month, 1)),
    };
  }
  const day = Number(dayMatch[1]);

  return {
    start: new Date(Date.UTC(year, month - 1, day)),
    end: new Date(Date.UTC(year, month - 1, day + 1)),
  };
}

export function selectPartitionFiles(keys, range) {
  return keys.filter((key) => {
    if (!key.endsWith('.parquet')) {
      return false;
    }

    const interval = getPartitionInterval(key);
    if (!interval) {
      return true;
    }

    const startsBeforeRangeEnds =
      range.to === null || interval.start <= range.to;
    const endsAfterRangeStarts =
      range.from === null || interval.end > range.from;

    return startsBeforeRangeEnds && endsAfterRangeStarts;
  });
}
