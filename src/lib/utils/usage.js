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

export const USAGE_SUM_FIELDS = {
  bytes: 'storage.bytes',
  objects: 'storage.objects',
  partitions: 'storage.partitions',
  queries: 'access.count',
  sinceLastFetch: 'access.sinceLastFetch',
  refreshDurationMs: 'lastRefresh.durationMs',
  refreshBytesFetched: 'lastRefresh.bytesFetched',
};

export function summarizeUsage(groups) {
  const fields = ['fdas', ...Object.keys(USAGE_SUM_FIELDS)];
  const totals = Object.fromEntries(fields.map((field) => [field, 0]));
  totals.lastAccessAt = null;
  for (const group of groups) {
    for (const field of fields) {
      totals[field] += group[field] ?? 0;
    }
    if (
      group.lastAccessAt &&
      (!totals.lastAccessAt ||
        new Date(group.lastAccessAt) > new Date(totals.lastAccessAt))
    ) {
      totals.lastAccessAt = group.lastAccessAt;
    }
  }
  return totals;
}
