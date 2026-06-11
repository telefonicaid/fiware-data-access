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

import { test, expect } from '@jest/globals';

export function registerFdaQueryPerformanceTests({
  getBaseUrl,
  service,
  servicePath,
  fdaId,
  httpReq,
  buildDaDataUrl,
}) {
  describe('DA data queries', () => {
    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data from basic FDA', async () => {
      const baseUrl = getBaseUrl();

      performance.mark('query-start');
      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, 'defaultDataAccess', {
          age: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data NDJSON failed:',
          queryRes.status,
          queryRes.text,
        );
      }

      expect(queryRes.status).toBe(200);
      expect(queryRes.text).toBeDefined();

      performance.mark('query-end');
      performance.measure('basic-query', 'query-start', 'query-end');

      const totalTime = performance.getEntriesByName('basic-query')[0];

      console.log(
        `[PERF] Basic DA query took ${totalTime.duration.toFixed(2)}ms`,
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data from compressed FDA', async () => {
      const baseUrl = getBaseUrl();

      performance.mark('query-start');
      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          `${fdaId}-compressed`,
          'defaultDataAccess',
          {
            age: 25,
          },
        ),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data NDJSON failed:',
          queryRes.status,
          queryRes.text,
        );
      }

      expect(queryRes.status).toBe(200);
      expect(queryRes.text).toBeDefined();

      performance.mark('query-end');
      performance.measure('compressed-query', 'query-start', 'query-end');

      const totalTime = performance.getEntriesByName('compressed-query')[0];

      console.log(
        `[PERF] Compressed DA query took ${totalTime.duration.toFixed(2)}ms`,
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data from partitioned FDA', async () => {
      const baseUrl = getBaseUrl();

      performance.mark('query-start');
      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          `${fdaId}-partitioned`,
          'defaultDataAccess',
          {
            age: 25,
          },
        ),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data NDJSON failed:',
          queryRes.status,
          queryRes.text,
        );
      }

      expect(queryRes.status).toBe(200);
      expect(queryRes.text).toBeDefined();

      performance.mark('query-end');
      performance.measure('partitioned-query', 'query-start', 'query-end');

      const totalTime = performance.getEntriesByName('partitioned-query')[0];

      console.log(
        `[PERF] Partitioned DA query took ${totalTime.duration.toFixed(2)}ms`,
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data from partitioned FDA (date based query)', async () => {
      const baseUrl = getBaseUrl();

      performance.mark('query-start');
      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          `${fdaId}-partitioned`,
          'defaultDataAccess',
          {
            timeinstant: new Date(
              Date.now() - 2 * 24 * 60 * 60 * 1000,
            ).toISOString(),
          },
        ),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data NDJSON failed:',
          queryRes.status,
          queryRes.text,
        );
      }

      expect(queryRes.status).toBe(200);
      expect(queryRes.text).toBeDefined();

      performance.mark('query-end');
      performance.measure(
        'partitioned-dateBased-query',
        'query-start',
        'query-end',
      );

      const totalMeasure = performance.getEntriesByName(
        'partitioned-dateBased-query',
      )[0];

      console.log(
        `[PERF] Partitioned DA query (date based) took ${totalMeasure.duration.toFixed(2)}ms`,
      );
    });
  });
}
