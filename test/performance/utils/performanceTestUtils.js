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

export const PERFORMANCE_TABLE_ROWS_ARG = '--performanceTableRows=';
const DEFAULT_PERFORMANCE_TABLE_ROWS = 1000;

export function parsePerformanceTableRows(rawValue) {
  if (rawValue == null || rawValue === '') {
    return DEFAULT_PERFORMANCE_TABLE_ROWS;
  }

  const normalized = String(rawValue).trim();
  const candidate = Number(normalized);
  if (!Number.isInteger(candidate) || candidate <= 0) {
    console.warn(
      `[TEST] Ignoring invalid performanceTableRows value: ${normalized}`,
    );
    return DEFAULT_PERFORMANCE_TABLE_ROWS;
  }

  return candidate;
}

export async function waitUntilFDAStatus({
  baseUrl,
  service,
  fdaId,
  visibility = 'public',
  timeout = 10000,
  interval = 300,
  status,
  progress,
  httpReq,
}) {
  const start = Date.now();
  let lastSeen;

  while (Date.now() - start < timeout) {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${encodeURIComponent(fdaId)}`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status === 200 && res.json) {
      lastSeen = { status: res.json.status, progress: res.json.progress };

      if (
        res.json.status === status &&
        (progress === undefined || res.json.progress === progress)
      ) {
        return res.json;
      }

      if (res.json.progress > progress) {
        console.log(
          `[TEST] FDA status update was faster than check status interval. Last seen status=${res.json.status}, progress=${res.json.progress}`,
        );
        return res.json;
      }
    }

    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  throw new Error(
    `Timeout waiting for FDA ${fdaId} to reach completed state (last status=${lastSeen?.status ?? 'unknown'}, progress=${lastSeen?.progress ?? 'unknown'}, error=${lastSeen?.error ?? 'n/a'})`,
  );
}
