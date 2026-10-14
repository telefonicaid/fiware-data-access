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

export function normalizeDownloadPolicy(
  { maxDownloads, expirationDate, validityPeriod },
  now = new Date(),
) {
  if (
    maxDownloads !== undefined &&
    (!Number.isSafeInteger(maxDownloads) || maxDownloads <= 0)
  ) {
    throw new FDAError(
      400,
      'InvalidDownloadPolicy',
      'maxDownloads must be a positive safe integer',
    );
  }
  if (expirationDate !== undefined && validityPeriod !== undefined) {
    throw new FDAError(
      400,
      'InvalidDownloadPolicy',
      'Specify expirationDate or validityPeriod, not both',
    );
  }

  let expiresAt;
  if (expirationDate !== undefined) {
    const timestampPattern =
      /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
    if (
      typeof expirationDate !== 'string' ||
      !timestampPattern.test(expirationDate)
    ) {
      throw new FDAError(
        400,
        'InvalidDownloadPolicy',
        'expirationDate must be an ISO 8601 timestamp with a timezone',
      );
    }
    const calendarDate = new Date(`${expirationDate.slice(0, 10)}T00:00:00Z`);
    if (
      Number.isNaN(calendarDate.getTime()) ||
      calendarDate.toISOString().slice(0, 10) !== expirationDate.slice(0, 10)
    ) {
      throw new FDAError(
        400,
        'InvalidDownloadPolicy',
        'expirationDate contains an invalid calendar date',
      );
    }
    expiresAt = new Date(expirationDate);
  }
  if (validityPeriod !== undefined) {
    const duration =
      typeof validityPeriod === 'string' &&
      /^P(?:(\d+)D)?(?:T(\d+)H)?$/.exec(validityPeriod);
    const durationMs =
      duration &&
      (Number(duration[1] ?? 0) * 24 + Number(duration[2] ?? 0)) * 3600000;
    if (!Number.isSafeInteger(durationMs) || durationMs <= 0) {
      throw new FDAError(
        400,
        'InvalidDownloadPolicy',
        'validityPeriod must be a positive ISO 8601 duration in days and hours, for example P7D or P1DT12H',
      );
    }
    expiresAt = new Date(now.getTime() + durationMs);
  }
  if (
    expiresAt &&
    (!Number.isFinite(expiresAt.getTime()) || expiresAt <= now)
  ) {
    throw new FDAError(
      400,
      'InvalidDownloadPolicy',
      'expirationDate must be in the future',
    );
  }
  if (maxDownloads === undefined && !expiresAt) {
    return undefined;
  }
  return {
    ...(maxDownloads !== undefined && { maxDownloads }),
    ...(expiresAt && { expirationDate: expiresAt }),
  };
}

export function getDownloadMetadata(fda, now = new Date()) {
  if (!fda.downloadPolicy) {
    return {};
  }
  const { maxDownloads, expirationDate } = fda.downloadPolicy;
  const downloadsUsed = fda.downloadsUsed ?? 0;
  const remainingDownloads =
    maxDownloads === undefined
      ? null
      : Math.max(0, maxDownloads - downloadsUsed);
  const expired = expirationDate && new Date(expirationDate) <= now;
  return {
    maxDownloads: maxDownloads ?? null,
    expirationDate: expirationDate ?? null,
    downloadsUsed,
    remainingDownloads,
    downloadStatus: expired
      ? 'expired'
      : remainingDownloads === 0
        ? 'exhausted'
        : 'available',
  };
}
