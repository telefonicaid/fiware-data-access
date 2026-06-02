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

import { describe, test, expect } from '@jest/globals';

export function registerQueryStyleDataIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqRaw,
  buildQueryStyleDaDataUrl,
  buildQueryStyleFdaDataUrl,
  waitUntilFDACompleted,
}) {
  describe('Query-style data endpoints', () => {
    const fixtureFdaId = 'fda_qs_dq_fixture';
    const fixtureDaId = 'da_qs_dq_fixture';

    test('GET /data/da supports query-style URL-only execution', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildQueryStyleDaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fixtureFdaId,
          daId: fixtureDaId,
          params: { minAge: 25 },
        }),
        headers: { Accept: 'application/json' },
      });

      expect(res.status).toBe(200);
      expect(res.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /data/da supports outputType query param', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReqRaw({
        method: 'GET',
        url: buildQueryStyleDaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fixtureFdaId,
          daId: fixtureDaId,
          outputType: 'csv',
          params: { minAge: 25 },
        }),
      });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain('text/csv');
      expect(res.text).toContain('id,name,age');
    });

    test('GET /data/da returns 409 when query-style is mixed with legacy headers', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildQueryStyleDaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fixtureFdaId,
          daId: fixtureDaId,
          params: { minAge: 25 },
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/json',
        },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('RequestStyleConflict');
    });

    test('GET /data/da rejects invalid outputType query param', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildQueryStyleDaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fixtureFdaId,
          daId: fixtureDaId,
          outputType: 'html',
          params: { minAge: 25 },
        }),
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toContain("Invalid outputType 'html'");
    });

    test('GET /data/fda supports query-style URL-only execution', async () => {
      const baseUrl = getBaseUrl();
      const fdaQueryStyleId = 'fda_query_style_direct';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaQueryStyleId,
          description: 'query-style direct endpoint test',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const freshRes = await httpReq({
        method: 'GET',
        url: buildQueryStyleFdaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fdaQueryStyleId,
        }),
      });

      expect(freshRes.status).toBe(200);
      expect(Array.isArray(freshRes.json)).toBe(true);
      expect(freshRes.json.length).toBeGreaterThan(0);
    });

    test('GET /data/fda supports outputType query param', async () => {
      const baseUrl = getBaseUrl();
      const fdaQueryStyleCsvId = 'fda_query_style_csv';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaQueryStyleCsvId,
          description: 'query-style csv endpoint test',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const freshRes = await httpReqRaw({
        method: 'GET',
        url: buildQueryStyleFdaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fdaQueryStyleCsvId,
          outputType: 'csv',
        }),
      });

      expect(freshRes.status).toBe(200);
      expect(freshRes.headers['content-type']).toContain('text/csv');
      expect(freshRes.text).toContain('id,name,age,timeinstant,authorized');
    });

    test('GET /data/fda returns 409 when query-style is mixed with legacy headers', async () => {
      const baseUrl = getBaseUrl();
      const fdaConflictId = 'fda_query_style_conflict';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaConflictId,
          description: 'query-style conflict test',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const freshRes = await httpReq({
        method: 'GET',
        url: buildQueryStyleFdaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fdaConflictId,
        }),
        headers: {
          'Fiware-Service': service,
        },
      });

      expect(freshRes.status).toBe(409);
      expect(freshRes.json.error).toBe('RequestStyleConflict');
    });

    test('GET /data/fda rejects invalid outputType query param', async () => {
      const baseUrl = getBaseUrl();
      const fdaInvalidOutputTypeId = 'fda_query_style_invalid_output_type';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaInvalidOutputTypeId,
          description: 'query-style invalid outputType test',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const freshRes = await httpReq({
        method: 'GET',
        url: buildQueryStyleFdaDataUrl(baseUrl, {
          service,
          servicePath,
          visibility,
          fdaId: fdaInvalidOutputTypeId,
          outputType: 'html',
        }),
      });

      expect(freshRes.status).toBe(400);
      expect(freshRes.json.error).toBe('BadRequest');
      expect(freshRes.json.description).toContain("Invalid outputType 'html'");
    });
  });
}
