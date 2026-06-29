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

import { describe, beforeAll, test, expect } from '@jest/globals';

export function registerQueryStyleDataIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  getPgHost,
  getPgPort,
  httpReq,
  httpReqRaw,
  buildDaDataUrl,
  buildFdaDataUrl,
  waitUntilFDACompleted,
}) {
  describe('Query-style data endpoints', () => {
    const fixtureFdaId = 'fda_qs_dq_fixture';
    const fixtureDaId = 'da_qs_dq_fixture';

    async function ensureDefaultDatasource(baseUrl) {
      const createRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/datasources`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          datasourceId: 'default',
          type: 'postgres',
          config: {
            user: 'postgres',
            password: 'postgres',
            host: getPgHost(),
            port: getPgPort(),
            database: service,
          },
        },
      });

      if (createRes.status !== 200 && createRes.status !== 409) {
        throw new Error(
          `Failed to ensure default datasource: ${createRes.status} ${JSON.stringify(createRes.json)}`,
        );
      }
    }

    beforeAll(async () => {
      const baseUrl = getBaseUrl();

      await ensureDefaultDatasource(baseUrl);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fixtureFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'query-style da data fixture',
        },
      });

      if (createFda.status !== 202) {
        throw new Error(
          `Failed to create query-style DA fixture FDA: ${createFda.status} ${JSON.stringify(createFda.json)}`,
        );
      }

      await waitUntilFDACompleted({ baseUrl, service, fdaId: fixtureFdaId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fixtureFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: fixtureDaId,
          description: 'query-style da filter fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      if (createDa.status !== 200) {
        throw new Error(
          `Failed to create query-style DA fixture: ${createDa.status} ${JSON.stringify(createDa.json)}`,
        );
      }
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data supports query-style context in query params', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          service,
          servicePath,
          minAge: 25,
        }),
        headers: { Accept: 'application/json' },
      });

      expect(res.status).toBe(200);
      expect(res.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data supports outputType query param in query-style mode', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          service,
          servicePath,
          outputType: 'csv',
          minAge: 25,
        }),
      });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain('text/csv');
      expect(res.text).toContain('id,name,age');
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data supports outputType=cda in query-style mode', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          service,
          servicePath,
          outputType: 'cda',
          minAge: 25,
        }),
      });

      expect(res.status).toBe(200);
      expect(res.json).toHaveProperty('metadata');
      expect(res.json).toHaveProperty('resultset');
      expect(res.json).toHaveProperty('queryInfo');
      expect(res.json.metadata).toEqual([
        { colIndex: 0, colName: 'id' },
        { colIndex: 1, colName: 'name' },
        { colIndex: 2, colName: 'age' },
      ]);
      expect(res.json.resultset).toEqual([
        ['1', 'ana', '30'],
        ['3', 'carlos', '40'],
      ]);
      expect(res.json.queryInfo.pageStart).toBe(0);
      expect(res.json.queryInfo.pageSize).toBe(2);
      expect(res.json.queryInfo.totalRows).toBe(2);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns 409 when query-style is mixed with legacy headers', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          service,
          servicePath,
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/json',
        },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('RequestStyleConflict');
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data rejects invalid outputType query param in query-style mode', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          service,
          servicePath,
          outputType: 'html',
          minAge: 25,
        }),
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toContain("Invalid outputType 'html'");
    });

    test('GET /{visibility}/fdas/{fdaId}/data supports query-style context in query params', async () => {
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
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaQueryStyleId)}?service=${encodeURIComponent(service)}&servicePath=${encodeURIComponent(servicePath)}`,
      });

      expect(freshRes.status).toBe(200);
      expect(Array.isArray(freshRes.json)).toBe(true);
      expect(freshRes.json.length).toBeGreaterThan(0);
    });

    test('GET /{visibility}/fdas/{fdaId}/data supports outputType query param in query-style mode', async () => {
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
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaQueryStyleCsvId)}?service=${encodeURIComponent(service)}&servicePath=${encodeURIComponent(servicePath)}&outputType=csv`,
      });

      expect(freshRes.status).toBe(200);
      expect(freshRes.headers['content-type']).toContain('text/csv');
      expect(freshRes.text).toContain('id,name,age,timeinstant,authorized');
    });

    test('GET /{visibility}/fdas/{fdaId}/data supports outputType=cda in query-style mode', async () => {
      const baseUrl = getBaseUrl();
      const fdaQueryStyleCDAJsonId = 'fda_query_style_cda_json';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaQueryStyleCDAJsonId,
          description: 'query-style cda endpoint test',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const freshRes = await httpReq({
        method: 'GET',
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaQueryStyleCDAJsonId)}?service=${encodeURIComponent(service)}&servicePath=${encodeURIComponent(servicePath)}&outputType=cda`,
      });

      expect(freshRes.status).toBe(200);
      expect(freshRes.json).toHaveProperty('metadata');
      expect(freshRes.json).toHaveProperty('resultset');
      expect(freshRes.json).toHaveProperty('queryInfo');
      expect(Array.isArray(freshRes.json.resultset)).toBe(true);
      expect(freshRes.json.resultset.length).toBeGreaterThan(0);
      expect(freshRes.json.queryInfo.pageStart).toBe(0);
      expect(freshRes.json.queryInfo.pageSize).toBe(3);
      expect(freshRes.json.queryInfo.totalRows).toBe(3);
    });

    test('GET /{visibility}/fdas/{fdaId}/data returns 409 when query-style is mixed with legacy headers', async () => {
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
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaConflictId)}?service=${encodeURIComponent(service)}&servicePath=${encodeURIComponent(servicePath)}`,
        headers: {
          'Fiware-Service': service,
        },
      });

      expect(freshRes.status).toBe(409);
      expect(freshRes.json.error).toBe('RequestStyleConflict');
    });

    test('GET /{visibility}/fdas/{fdaId}/data rejects invalid outputType query param in query-style mode', async () => {
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
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaInvalidOutputTypeId)}?service=${encodeURIComponent(service)}&servicePath=${encodeURIComponent(servicePath)}&outputType=html`,
      });

      expect(freshRes.status).toBe(400);
      expect(freshRes.json.error).toBe('BadRequest');
      expect(freshRes.json.description).toContain("Invalid outputType 'html'");
    });
  });
}
