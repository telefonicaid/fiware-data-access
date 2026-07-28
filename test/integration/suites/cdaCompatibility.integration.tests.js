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
import { Client } from 'pg';
import { connectWithRetry } from '../utils/integrationTestUtils.js';

export function registerCdaCompatibilityIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpFormReq,
  httpReqRaw,
  waitUntilFDACompleted,
  fdaId,
  daId,
  getPgHost,
  getPgPort,
}) {
  describe('CDA compatibility', () => {
    test('POST /plugin/cda/api/doQuery behaves as CDA compatibility layer', async () => {
      const baseUrl = getBaseUrl();
      const cdaFdaId = 'fdaID_da_cda';
      const cdaDaId = 'fda_da_cda';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: cdaFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset for CDA',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: cdaFdaId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${cdaFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: cdaDaId,
          description: 'CDA test DA',
          query: `
          SELECT id, name, age, COUNT(*) OVER() AS __total
          WHERE age >= $minAge
          ORDER BY id
          LIMIT $pageSize OFFSET $pageStart
        `,
          params: [
            { name: 'minAge', type: 'Number', default: 0 },
            { name: 'pageSize', type: 'Number', default: 10 },
            { name: 'pageStart', type: 'Number', default: 0 },
          ],
        },
      });

      expect(createDa.status).toBe(204);

      const res = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${cdaFdaId}`,
          dataAccessId: cdaDaId,
          paramminAge: '0',
          pageSize: '2',
          pageStart: '0',
        },
      });

      if (res.status >= 400) {
        console.error('CDA doQuery failed:', res.status, res.json ?? res.text);
      }

      expect(res.status).toBe(200);
      expect(res.json).toHaveProperty('metadata');
      expect(res.json).toHaveProperty('resultset');
      expect(res.json).toHaveProperty('queryInfo');
      expect(Array.isArray(res.json.metadata)).toBe(true);
      expect(res.json.metadata[0]).toHaveProperty('colIndex');
      expect(res.json.metadata[0]).toHaveProperty('colName');
      expect(Array.isArray(res.json.resultset)).toBe(true);
      expect(Array.isArray(res.json.resultset[0])).toBe(true);
      expect(res.json.resultset.length).toBe(2);
      expect(res.json.queryInfo.pageStart).toBe(0);
      expect(res.json.queryInfo.pageSize).toBe(2);
      expect(res.json.queryInfo.totalRows).toBe(3);

      const ndjsonAttempt = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
        form: {
          path: `/public/${service}/verticals/sql/${cdaFdaId}`,
          dataAccessId: cdaDaId,
          pageSize: '2',
          pageStart: '0',
        },
      });

      expect(ndjsonAttempt.status).toBe(200);
      expect(ndjsonAttempt.text.includes('\n')).toBe(false);
      expect(ndjsonAttempt.json).toHaveProperty('resultset');
    });

    test('GET /plugin/cda/api/doQuery supports query params without FIWARE headers', async () => {
      const baseUrl = getBaseUrl();
      const url = new URL(`${baseUrl}/plugin/cda/api/doQuery`);

      url.searchParams.set('path', `/public/${service}/verticals/sql/${fdaId}`);
      url.searchParams.set('dataAccessId', daId);
      url.searchParams.set('paramminAge', '25');
      url.searchParams.set('pageSize', '2');
      url.searchParams.set('pageStart', '0');

      const res = await httpReq({
        method: 'GET',
        url: url.toString(),
      });

      expect(res.status).toBe(200);
      expect(res.json).toHaveProperty('metadata');
      expect(res.json).toHaveProperty('resultset');
      expect(res.json).toHaveProperty('queryInfo');
      expect(res.json.queryInfo.pageStart).toBe(0);
      expect(res.json.queryInfo.pageSize).toBe(2);
    });

    test('GET /plugin/cda/api/doQuery supports legacy home path and outputType query param', async () => {
      const baseUrl = getBaseUrl();
      const url = new URL(`${baseUrl}/plugin/cda/api/doQuery`);

      url.searchParams.set(
        'path',
        `home/${service}/verticals/public/${fdaId}.cda`,
      );
      url.searchParams.set('dataAccessId', daId);
      url.searchParams.set('paramminAge', '25');
      url.searchParams.set('outputType', 'csv');
      url.searchParams.set('_TRUST_USER_', `opendata_${service}`);

      const res = await httpReqRaw({
        method: 'GET',
        url: url.toString(),
      });

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

    test('POST /plugin/cda/api/doQuery rejects scope mismatch', async () => {
      const baseUrl = getBaseUrl();
      const privateFdaId = 'fda_cda_scope_private';
      const privateDaId = 'da_cda_scope_private';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/private/fdas`,
        headers: { 'Fiware-Service': service },
        body: {
          id: privateFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset for CDA private scope',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: privateFdaId,
        visibility: 'private',
      });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/private/fdas/${privateFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: privateDaId,
          description: 'CDA scope mismatch test DA',
          query: `
          SELECT id, name, age
          WHERE age >= $minAge
          ORDER BY id
        `,
          params: [{ name: 'minAge', type: 'Number', default: 0 }],
        },
      });

      expect(createDa.status).toBe(204);

      const mismatchRes = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${privateFdaId}`,
          dataAccessId: privateDaId,
          paramminAge: '0',
        },
      });

      expect(mismatchRes.status).toBe(403);
      expect(mismatchRes.json.error).toBe('VisibilityMismatch');
    });

    test('POST /plugin/cda/api/doQuery supports outputType=csv', async () => {
      const baseUrl = getBaseUrl();
      const res = await httpReqRaw({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${fdaId}`,
          dataAccessId: daId,
          paramminAge: '25',
          outputType: 'csv',
        },
      });

      if (res.status >= 400) {
        console.error(
          'POST /plugin/cda/api/doQuery outputType=csv failed:',
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

    test('POST /plugin/cda/api/doQuery supports outputType=xls', async () => {
      const baseUrl = getBaseUrl();
      const res = await httpReqRaw({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${fdaId}`,
          dataAccessId: daId,
          paramminAge: '25',
          outputType: 'xls',
        },
      });

      if (res.status >= 400) {
        console.error(
          'POST /plugin/cda/api/doQuery outputType=xls failed:',
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

    test('POST /plugin/cda/api/doQuery serializes dates consistently for json/csv/xls', async () => {
      const baseUrl = getBaseUrl();
      const fixtureTable = 'cda_format_serialization_fixture';
      const cdaFdaId = 'fda_cda_serialization_regression';
      const cdaDaId = 'da_cda_serialization_regression';
      const expectedDates = [
        '2024-01-10T12:34:56.789Z',
        '2024-01-11T08:15:30.123Z',
      ];

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: service,
      });

      await connectWithRetry(pgClient);

      try {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.query(`
          CREATE TABLE public.${fixtureTable} (
            id INT PRIMARY KEY,
            observed_at TIMESTAMPTZ NOT NULL,
            total_bigint BIGINT NOT NULL,
            note TEXT
          )
        `);
        await pgClient.query(
          `
            INSERT INTO public.${fixtureTable} (id, observed_at, total_bigint, note)
            VALUES
              (1, $1::timestamptz, 42, 'alpha'),
              (2, $2::timestamptz, 84, 'with,comma')
          `,
          expectedDates,
        );

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: cdaFdaId,
            description: 'cda serialization regression fda',
            query: `
              SELECT
                id,
                observed_at AS date,
                total_bigint AS total,
                note
              FROM public.${fixtureTable}
              ORDER BY id
            `,
          },
        });

        expect(createFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: cdaFdaId,
        });

        const createDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${cdaFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: cdaDaId,
            description: 'cda serialization regression da',
            query: `
              SELECT date, total, note
              ORDER BY date
            `,
          },
        });

        expect(createDa.status).toBe(204);

        const jsonRes = await httpFormReq({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
            dataAccessId: cdaDaId,
            pageSize: '10',
            pageStart: '0',
          },
        });

        expect(jsonRes.status).toBe(200);
        expect(Array.isArray(jsonRes.json.metadata)).toBe(true);
        expect(Array.isArray(jsonRes.json.resultset)).toBe(true);

        const dateColIndex = jsonRes.json.metadata.findIndex(
          (col) => col.colName === 'date',
        );
        const totalColIndex = jsonRes.json.metadata.findIndex(
          (col) => col.colName === 'total',
        );

        expect(dateColIndex).toBeGreaterThanOrEqual(0);
        expect(totalColIndex).toBeGreaterThanOrEqual(0);
        expect(jsonRes.json.resultset.map((row) => row[dateColIndex])).toEqual(
          expectedDates,
        );
        expect(
          jsonRes.json.resultset.every((row) =>
            ['string', 'number'].includes(typeof row[totalColIndex]),
          ),
        ).toBe(true);

        const csvRes = await httpReqRaw({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
            dataAccessId: cdaDaId,
            outputType: 'csv',
          },
        });

        expect(csvRes.status).toBe(200);
        expect(csvRes.headers['content-type']).toContain('text/csv');
        expect(csvRes.text).toContain(expectedDates[0]);
        expect(csvRes.text).toContain(expectedDates[1]);
        expect(csvRes.text).not.toContain('[object Object]');

        const xlsRes = await httpReqRaw({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
            dataAccessId: cdaDaId,
            outputType: 'xls',
          },
        });

        expect(xlsRes.status).toBe(200);
        expect(xlsRes.headers['content-type']).toContain(
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        );
        expect(xlsRes.headers['content-disposition']).toContain('results.xlsx');
        expect(xlsRes.buffer[0]).toBe(0x50);
        expect(xlsRes.buffer[1]).toBe(0x4b);
        expect(xlsRes.buffer.length).toBeGreaterThan(100);
      } finally {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.end();
      }
    });

    test('POST /plugin/cda/api/doQuery rejects unsupported outputType', async () => {
      const baseUrl = getBaseUrl();
      const res = await httpReqRaw({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${fdaId}`,
          dataAccessId: daId,
          outputType: 'xml',
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toContain('Invalid outputType');
    });
  });
}
