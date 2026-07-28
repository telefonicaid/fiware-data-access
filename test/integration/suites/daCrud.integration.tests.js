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

export function registerDaCrudIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  buildDaDataUrl,
  fdaId,
  daId,
}) {
  describe('DA CRUD and execution', () => {
    test('POST /fdas/:fdaId/das + GET /{visibility}/fdas/{fdaId}/das/{daId}/data executes DuckDB against Parquet', async () => {
      const baseUrl = getBaseUrl();
      // DuckDB reads parquet generated in  s3://<bucket>/<fdaID>.parquet
      const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge
      ORDER BY id;
    `;

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daId,
          description: 'age filter',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      if (createDa.status >= 400) {
        console.error(
          'POST /das failed:',
          createDa.status,
          createDa.json ?? createDa.text,
        );
      }
      expect(createDa.status).toBe(204);

      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed:',
          queryRes.status,
          queryRes.json ?? queryRes.text,
        );
      }
      expect(queryRes.status).toBe(200);
      expect(queryRes.json).toEqual([
        { id: 1, name: 'ana', age: 30 },
        { id: 3, name: 'carlos', age: 40 },
      ]);
    });

    test('GET /fdas/:fdaId/das and GET /fdas/:fdaId/das/:daId return stored DA and DaNotFound for unknown DA', async () => {
      const baseUrl = getBaseUrl();

      const listRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
      });

      if (listRes.status >= 400) {
        console.error(
          'GET /fdas/:fdaId/das failed:',
          listRes.status,
          listRes.json ?? listRes.text,
        );
      }

      expect(listRes.status).toBe(200);
      expect(Array.isArray(listRes.json)).toBe(true);
      expect(listRes.json.some((x) => x.id === daId)).toBe(true);

      const getRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}`,
        headers: { 'Fiware-Service': service },
      });

      expect(getRes.status).toBe(200);
      expect(getRes.json).toMatchObject({
        id: daId,
        description: 'age filter',
      });

      const missingRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/da_does_not_exist`,
        headers: { 'Fiware-Service': service },
      });

      expect(missingRes.status).toBe(404);
      expect(missingRes.json.error).toBe('DaNotFound');
    });

    test('POST /fdas/:fdaId/das rejects query with FROM clause', async () => {
      const baseUrl = getBaseUrl();
      const badQuery = `
      FROM read_parquet('s3://some/path')
      SELECT id
    `;

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: 'da_bad',
          description: 'should fail',
          query: badQuery,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidDAQuery');
    });

    test('PUT /fdas/:fdaId/das/:daId updates DA query and affects execution', async () => {
      const baseUrl = getBaseUrl();
      const daIdToUpdate = 'da_update';

      const createRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daIdToUpdate,
          description: 'initial filter',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      expect(createRes.status).toBe(204);

      const firstQuery = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daIdToUpdate, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(firstQuery.status).toBe(200);
      expect(firstQuery.json.length).toBe(2);

      const updateRes = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          description: 'updated filter',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          AND age < 35
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      if (updateRes.status >= 400) {
        console.error(
          'PUT /das failed:',
          updateRes.status,
          updateRes.json ?? updateRes.text,
        );
      }

      expect(updateRes.status).toBe(204);

      const secondQuery = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daIdToUpdate, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(secondQuery.status).toBe(200);
      expect(secondQuery.json).toEqual([{ id: 1, name: 'ana', age: 30 }]);
    });
  });
}
