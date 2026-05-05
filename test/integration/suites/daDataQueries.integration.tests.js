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

export function registerDaDataQueriesIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqRaw,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  describe('DA data queries', () => {
    const fixtureFdaId = 'fda_dq_fixture';
    const fixtureDaId = 'da_dq_fixture';

    beforeAll(async () => {
      const baseUrl = getBaseUrl();

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
          description: 'da data queries fixture',
        },
      });

      if (createFda.status !== 202) {
        throw new Error(
          `Failed to create fixture FDA: ${createFda.status} ${JSON.stringify(createFda.json)}`,
        );
      }

      await waitUntilFDACompleted({ baseUrl, service, fdaId: fixtureFdaId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fixtureFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: fixtureDaId,
          description: 'age filter fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      if (createDa.status !== 201) {
        throw new Error(
          `Failed to create fixture DA: ${createDa.status} ${JSON.stringify(createDa.json)}`,
        );
      }
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns JSON array when Accept: application/json', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service, Accept: 'application/json' },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data (json) failed:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(200);
      expect(res.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data rejects fresh query param', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          minAge: 25,
          fresh: 'true',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns NDJSON when Accept: application/x-ndjson', async () => {
      const baseUrl = getBaseUrl();

      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          minAge: 25,
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

      const lines = queryRes.text.split('\n').filter((line) => line.trim());
      expect(lines.length).toBe(2);

      const row1 = JSON.parse(lines[0]);
      const row2 = JSON.parse(lines[1]);

      expect(row1).toEqual({ id: 1, name: 'ana', age: 30 });
      expect(row2).toEqual({ id: 3, name: 'carlos', age: 40 });
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns CSV when Accept: text/csv', async () => {
      const baseUrl = getBaseUrl();
      const fdaCsvId = 'fda_accept_csv';
      const daCsvId = 'da_accept_csv';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaCsvId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept csv fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaCsvId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaCsvId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daCsvId,
          description: 'accept csv da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaCsvId, daCsvId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'text/csv',
        },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data Accept=text/csv failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain('text/csv');
      expect(res.headers['content-disposition']).toContain('results.csv');

      const lines = res.text
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
      expect(lines[0]).toBe('id,name,age');
      expect(lines[1]).toBe('1,ana,30');
      expect(lines[2]).toBe('3,carlos,40');
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns XLSX when Accept requests spreadsheet mime', async () => {
      const baseUrl = getBaseUrl();
      const fdaXlsId = 'fda_accept_xls';
      const daXlsId = 'da_accept_xls';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaXlsId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept xls fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaXlsId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaXlsId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daXlsId,
          description: 'accept xls da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaXlsId, daXlsId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept:
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data Accept=xlsx failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain(
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      expect(res.headers['content-disposition']).toContain('results.xlsx');
      expect(res.buffer[0]).toBe(0x50);
      expect(res.buffer[1]).toBe(0x4b);
      expect(res.buffer.length).toBeGreaterThan(100);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data rejects outputType query param', async () => {
      const baseUrl = getBaseUrl();
      const fdaJsonId = 'fda_accept_json';
      const daJsonId = 'da_accept_json';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaJsonId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept json fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaJsonId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaJsonId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daJsonId,
          description: 'accept json da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaJsonId, daJsonId, {
          minAge: 25,
          outputType: 'html',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toBe(
        'Invalid fields in request query, check your request',
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns 406 for unsupported Accept header', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fixtureFdaId, fixtureDaId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'video/mpg4',
        },
      });

      expect(res.status).toBe(406);
      expect(res.json.error).toBe('NotAcceptable');
      expect(res.json.description).toContain(
        'Accept header must allow application/json',
      );
    });
  });
}
