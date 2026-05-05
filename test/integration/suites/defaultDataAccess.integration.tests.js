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

export function registerDefaultDataAccessIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  test('defaultDataAccess without timeColumn includes pageSize/pageStart but not start/finish', async () => {
    const baseUrl = getBaseUrl();
    const untimedFdaId = 'fda_default_da_untimed';

    try {
      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: untimedFdaId,
          query:
            'SELECT id, name, age, authorized FROM public.users ORDER BY id',
          description: 'default DA untimed test',
        },
      });

      expect(createFda.status).toBe(202);
      const completedFDA = await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: untimedFdaId,
      });

      const defaultDa = completedFDA?.das?.defaultDataAccess;
      expect(defaultDa).toBeDefined();
      expect(defaultDa.query).toContain('COUNT(*) OVER() as __total');
      expect(defaultDa.query).toContain(
        'LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      );
      expect(defaultDa.query).not.toContain('$start');
      expect(defaultDa.query).not.toContain('$finish');
      expect(defaultDa.params.some((p) => p.name === 'pageSize')).toBe(true);
      expect(defaultDa.params.some((p) => p.name === 'pageStart')).toBe(true);
      expect(defaultDa.params.some((p) => p.name === 'start')).toBe(false);
      expect(defaultDa.params.some((p) => p.name === 'finish')).toBe(false);

      const pagingRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          untimedFdaId,
          'defaultDataAccess',
          { pageSize: 1, pageStart: 2 },
        ),
        headers: { 'Fiware-Service': service },
      });

      expect(pagingRes.status).toBe(200);
      expect(pagingRes.json).toHaveLength(1);
      expect(pagingRes.json[0].id).toBe('3');
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${untimedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });

  test('defaultDataAccess includes start/finish and pageSize/pageStart when FDA has timeColumn', async () => {
    const baseUrl = getBaseUrl();
    const timedFdaId = 'fda_default_da_timed';

    try {
      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: timedFdaId,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'default DA timed test',
          timeColumn: 'timeinstant',
        },
      });

      expect(createFda.status).toBe(202);
      const completedFDA = await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: timedFdaId,
      });

      const defaultDa = completedFDA?.das?.defaultDataAccess;
      expect(defaultDa).toBeDefined();
      expect(defaultDa.query).toContain(
        '($start IS NULL OR CAST("timeinstant" AS TIMESTAMP) >= CAST($start AS TIMESTAMP))',
      );
      expect(defaultDa.query).toContain(
        '($finish IS NULL OR CAST("timeinstant" AS TIMESTAMP) <= CAST($finish AS TIMESTAMP))',
      );
      expect(defaultDa.query).toContain('COUNT(*) OVER() as __total');
      expect(defaultDa.query).toContain(
        'LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      );
      expect(defaultDa.params.some((p) => p.name === 'start')).toBe(true);
      expect(defaultDa.params.some((p) => p.name === 'finish')).toBe(true);
      expect(defaultDa.params.some((p) => p.name === 'pageSize')).toBe(true);
      expect(defaultDa.params.some((p) => p.name === 'pageStart')).toBe(true);
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${timedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });

  test('defaultDataAccess supports start/finish and pageSize/pageStart in cached mode', async () => {
    const baseUrl = getBaseUrl();
    const timedFdaId = 'fda_default_da_timed_data';

    try {
      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: timedFdaId,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'default DA timed data test',
          timeColumn: 'timeinstant',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: timedFdaId,
      });

      const rangeAndPagingRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          timedFdaId,
          'defaultDataAccess',
          {
            start: '2020-01-01T00:00:00.000Z',
            finish: '2020-12-31T23:59:59.000Z',
            pageSize: 1,
            pageStart: 1,
          },
        ),
        headers: { 'Fiware-Service': service },
      });

      if (rangeAndPagingRes.status >= 400) {
        console.error(
          'GET defaultDataAccess with start/finish/pageSize/pageStart failed:',
          rangeAndPagingRes.status,
          rangeAndPagingRes.json ?? rangeAndPagingRes.text,
        );
      }

      expect(rangeAndPagingRes.status).toBe(200);
      expect(Array.isArray(rangeAndPagingRes.json)).toBe(true);
      expect(rangeAndPagingRes.json).toHaveLength(1);
      expect(rangeAndPagingRes.json[0].id).toBe('2');
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${timedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });

  test('defaultDataAccess matches exact equality on declared timeColumn in cached mode', async () => {
    const baseUrl = getBaseUrl();
    const timedFdaId = 'fda_default_da_timed_equality';

    try {
      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: timedFdaId,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'default DA timed equality test',
          timeColumn: 'timeinstant',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: timedFdaId,
      });

      const equalityRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          timedFdaId,
          'defaultDataAccess',
          {
            timeinstant: '2020-08-17T18:25:28.332Z',
          },
        ),
        headers: { 'Fiware-Service': service },
      });

      if (equalityRes.status >= 400) {
        console.error(
          'GET defaultDataAccess with exact timeColumn equality failed:',
          equalityRes.status,
          equalityRes.json ?? equalityRes.text,
        );
      }

      expect(equalityRes.status).toBe(200);
      expect(Array.isArray(equalityRes.json)).toBe(true);
      expect(equalityRes.json.map((row) => row.id)).toEqual(['1', '2', '3']);
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${timedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });
}
