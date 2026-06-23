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

import { beforeAll, describe, expect, test } from '@jest/globals';
import { MongoClient } from 'mongodb';

export function registerMongoFdasIntegrationTests({
  getBaseUrl,
  getMongoUri,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqRaw,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  describe('Mongo cached FDAs', () => {
    const datasourceId = 'mongo-cache-ds';
    const fdaId = 'mongo_cached_fda';
    const daId = 'mongo_cached_da';
    const collectionName = 'mongo_cached_fda_events';

    beforeAll(async () => {
      const baseUrl = getBaseUrl();
      const mongoClient = new MongoClient(getMongoUri(), {
        serverSelectionTimeoutMS: 10_000,
      });

      await mongoClient.connect();
      try {
        const collection = mongoClient.db('test-db').collection(collectionName);
        await collection.deleteMany({});
        await collection.insertMany([
          {
            device: 'sensor-a',
            status: 'ok',
            reading: '21.5',
            site: 'lab',
          },
          {
            device: 'sensor-b',
            status: 'warn',
            reading: '19.2',
            site: 'lab',
          },
          {
            device: 'sensor-c',
            status: 'ok',
            reading: '30.1',
            site: 'remote',
          },
        ]);
      } finally {
        await mongoClient.close();
      }

      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/datasources/${datasourceId}`,
        headers: { 'Fiware-Service': service },
      });

      const createDatasourceRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/datasources`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          datasourceId,
          type: 'mongodb',
          config: {
            uri: getMongoUri(),
            database: 'test-db',
          },
        },
      });

      if (createDatasourceRes.status >= 400) {
        throw new Error(
          `Failed to create Mongo datasource: ${createDatasourceRes.status} ${JSON.stringify(createDatasourceRes.json)}`,
        );
      }

      const createFdaRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query: {
            collection: collectionName,
            filter: { site: 'lab' },
            projection: {
              device: 1,
              status: 1,
              reading: 1,
            },
          },
          description: 'mongo cached fda integration fixture',
          cached: true,
          datasourceId,
        },
      });

      if (createFdaRes.status >= 400) {
        throw new Error(
          `Failed to create Mongo FDA: ${createFdaRes.status} ${JSON.stringify(createFdaRes.json)}`,
        );
      }

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId,
        visibility,
      });

      const createDaRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daId,
          description: 'mongo cached da integration fixture',
          query: `
            SELECT device, status, reading
            WHERE status = $status
            ORDER BY device
          `,
          params: [{ name: 'status', type: 'Text', required: true }],
        },
      });

      if (createDaRes.status >= 400) {
        throw new Error(
          `Failed to create Mongo DA: ${createDaRes.status} ${JSON.stringify(createDaRes.json)}`,
        );
      }
    });

    test('POST /fdas rejects cached=false for Mongo datasource', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'mongo_fresh_not_allowed',
          query: {
            collection: collectionName,
            filter: { site: 'lab' },
            projection: {
              device: 1,
              status: 1,
              reading: 1,
            },
          },
          description: 'mongo fresh not allowed',
          cached: false,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain('only supports cached FDAs');
    });

    test('GET /fdas/{fdaId} exposes Mongo-specific metadata', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      expect(res.status).toBe(200);
      expect(res.json.datasourceId).toBe(datasourceId);
      expect(res.json.query.collection).toBe(collectionName);
      expect(res.json.query.projection).toEqual({
        device: 1,
        status: 1,
        reading: 1,
      });
      expect(res.json.cached).toBe(true);
    });

    test('GET /das/{daId}/data returns cached Mongo FDA rows through DuckDB/Parquet', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          status: 'ok',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (res.status >= 400) {
        console.error(
          'Mongo cached DA JSON query failed:',
          res.status,
          res.json,
        );
      }

      expect(res.status).toBe(200);
      expect(res.json).toEqual([
        { device: 'sensor-a', status: 'ok', reading: 21.5 },
      ]);
    });

    test('GET /das/{daId}/data returns CSV for cached Mongo FDA', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          status: 'warn',
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'text/csv',
        },
      });

      if (res.status >= 400) {
        console.error(
          'Mongo cached DA CSV query failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(String(res.headers['content-type'])).toContain('text/csv');
      expect(res.text).toContain('device,status,reading');
      expect(res.text).toContain('sensor-b,warn,19.2');
    });
  });
}
