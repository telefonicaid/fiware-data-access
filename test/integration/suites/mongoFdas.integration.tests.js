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

import { afterAll, beforeAll, describe, expect, test } from '@jest/globals';
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
  buildFdaDataUrl,
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

    test('GET /{fdaId}/data rejects direct queries against a cached Mongo FDA', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildFdaDataUrl(baseUrl, servicePath, fdaId),
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('FDANotOnlyFresh');
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

    test('Mongo cached FDA with no matching source rows keeps DA creation/query working with empty results', async () => {
      const baseUrl = getBaseUrl();
      const emptyFdaId = 'mongo_cached_fda_empty';
      const emptyDaId = 'mongo_cached_da_empty';

      try {
        const createFdaRes = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: emptyFdaId,
            query: {
              collection: collectionName,
              filter: { site: 'missing-site' },
              projection: {
                device: 1,
                status: 1,
                reading: 1,
              },
            },
            description: 'mongo cached empty fda integration fixture',
            cached: true,
            datasourceId,
          },
        });

        if (createFdaRes.status >= 400) {
          console.error(
            'Failed creating empty Mongo FDA fixture:',
            createFdaRes.status,
            createFdaRes.json ?? createFdaRes.text,
          );
        }
        expect(createFdaRes.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: emptyFdaId,
          visibility,
        });

        const createDaRes = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${emptyFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: emptyDaId,
            description: 'mongo cached empty da integration fixture',
            query: `
              SELECT device, status, reading
              WHERE status = $status
              ORDER BY device
            `,
            params: [{ name: 'status', type: 'Text', required: true }],
          },
        });

        if (createDaRes.status >= 400) {
          console.error(
            'Failed creating empty Mongo DA fixture:',
            createDaRes.status,
            createDaRes.json ?? createDaRes.text,
          );
        }
        expect(createDaRes.status).toBe(204);

        const queryRes = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(baseUrl, servicePath, emptyFdaId, emptyDaId, {
            status: 'ok',
          }),
          headers: { 'Fiware-Service': service },
        });

        if (queryRes.status >= 400) {
          console.error(
            'Mongo cached empty DA JSON query failed:',
            queryRes.status,
            queryRes.json ?? queryRes.text,
          );
        }

        expect(queryRes.status).toBe(200);
        expect(queryRes.json).toEqual([]);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${emptyFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas supports Mongo aggregation pipelines', async () => {
      const baseUrl = getBaseUrl();
      const aggFdaId = 'mongo_cached_fda_agg';
      const aggDaId = 'mongo_cached_da_agg';

      try {
        const createFdaRes = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: aggFdaId,
            query: {
              collection: collectionName,
              aggregation: [
                { $match: { site: 'lab' } },
                { $group: { _id: '$status', n: { $sum: 1 } } },
              ],
            },
            description: 'mongo cached aggregation fda integration fixture',
            cached: true,
            datasourceId,
          },
        });

        if (createFdaRes.status >= 400) {
          console.error(
            'Failed creating aggregation Mongo FDA fixture:',
            createFdaRes.status,
            createFdaRes.json ?? createFdaRes.text,
          );
        }
        expect(createFdaRes.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: aggFdaId,
          visibility,
        });

        const createDaRes = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${aggFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: aggDaId,
            description: 'mongo cached aggregation da integration fixture',
            query: `
              SELECT _id, n
              ORDER BY _id
            `,
            params: [],
          },
        });

        if (createDaRes.status >= 400) {
          console.error(
            'Failed creating aggregation Mongo DA fixture:',
            createDaRes.status,
            createDaRes.json ?? createDaRes.text,
          );
        }
        expect(createDaRes.status).toBe(204);

        const queryRes = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(baseUrl, servicePath, aggFdaId, aggDaId),
          headers: { 'Fiware-Service': service },
        });

        if (queryRes.status >= 400) {
          console.error(
            'Mongo cached aggregation DA JSON query failed:',
            queryRes.status,
            queryRes.json ?? queryRes.text,
          );
        }

        expect(queryRes.status).toBe(200);
        expect(queryRes.json).toEqual([
          { _id: 'ok', n: 1 },
          { _id: 'warn', n: 1 },
        ]);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${aggFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas rejects a cached strict FDA whose query does not declare its columns', async () => {
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
          id: 'mongo_undeclared_columns',
          query: {
            collection: collectionName,
            filter: { site: 'lab' },
          },
          description: 'mongo cached fda without projection',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain(
        'must declare their output columns',
      );
    });

    test('POST /fdas accepts a cached FDA without projection in unchecked mode', async () => {
      const baseUrl = getBaseUrl();
      const uncheckedFdaId = 'mongo_unchecked_columns';

      try {
        const res = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: uncheckedFdaId,
            query: {
              collection: collectionName,
              filter: { site: 'lab' },
            },
            description: 'mongo cached fda without projection, unchecked',
            cached: true,
            validationMode: 'unchecked',
            datasourceId,
          },
        });

        expect(res.status).toBe(202);
        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: uncheckedFdaId,
        });

        // The column set is sampled from the first document, and no schema is
        // persisted: an unchecked FDA opts out of the schema contract entirely
        const getFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${uncheckedFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(getFda.status).toBe(200);
        expect(getFda.json.schema).toBeUndefined();
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${uncheckedFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas rejects a cached strict FDA whose projection only excludes fields', async () => {
      const baseUrl = getBaseUrl();

      // An exclusion projection says what to remove, it does not enumerate the result
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'mongo_exclusion_projection',
          query: {
            collection: collectionName,
            filter: {},
            projection: { device: 0 },
          },
          description: 'mongo cached fda with exclusion projection',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain(
        'must declare their output columns',
      );
    });

    test('POST /fdas rejects an aggregation whose final stage does not declare columns', async () => {
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
          id: 'mongo_agg_no_shaping_stage',
          query: {
            collection: collectionName,
            aggregation: [{ $match: { site: 'lab' } }],
          },
          description: 'mongo cached aggregation without shaping stage',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain(
        'must declare their output columns',
      );
    });

    test('GET /fdas/{fdaId} reports null column types until the first fetch succeeds', async () => {
      const baseUrl = getBaseUrl();
      const failedFdaId = 'mongo_null_types_fda';

      try {
        // Partitioning by `device` (text) fails whilem aterializing
        // the FDA keeps the schema it was created with.
        const createRes = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: failedFdaId,
            query: {
              collection: collectionName,
              filter: { site: 'lab' },
              projection: { device: 1, status: 1, reading: 1 },
            },
            description: 'mongo fda whose first fetch fails',
            cached: true,
            datasourceId,
            timeColumn: 'device',
            objStgConf: { partition: 'day' },
          },
        });

        expect(createRes.status).toBe(202);

        const finished = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: failedFdaId,
          visibility,
        });

        expect(finished.status).toBe('failed');
        expect(finished.schema).toEqual([
          { name: 'device', type: null },
          { name: 'status', type: null },
          { name: 'reading', type: null },
        ]);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${failedFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('nested documents and arrays materialize as JSON text, dot notation flattens them', async () => {
      const baseUrl = getBaseUrl();
      const nestedCollectionName = 'mongo_nested_fda_events';
      const nestedFdaId = 'mongo_nested_fda';
      const dottedFdaId = 'mongo_nested_dotted_fda';

      const mongoClient = new MongoClient(getMongoUri(), {
        serverSelectionTimeoutMS: 10_000,
      });
      await mongoClient.connect();
      try {
        const collection = mongoClient
          .db('test-db')
          .collection(nestedCollectionName);
        await collection.deleteMany({});
        await collection.insertOne({
          label: 'nested_doc',
          device: { name: 'sensor-x', meta: { floor: 3 } },
          readings: [1, 2, 3],
        });
      } finally {
        await mongoClient.close();
      }

      try {
        const createNested = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: nestedFdaId,
            query: {
              collection: nestedCollectionName,
              filter: {},
              projection: { label: 1, device: 1, readings: 1 },
            },
            description: 'mongo nested fields fda',
            cached: true,
            datasourceId,
          },
        });

        expect(createNested.status).toBe(202);
        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: nestedFdaId,
          visibility,
        });

        const nestedFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${nestedFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        // A subdocument and an array each collapse into a single text column
        expect(nestedFda.json.schema).toEqual([
          { name: 'label', type: 'VARCHAR' },
          { name: 'device', type: 'VARCHAR' },
          { name: 'readings', type: 'VARCHAR' },
        ]);

        const nestedRows = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            nestedFdaId,
            'defaultDataAccess',
            { pageSize: 10, pageStart: 0 },
          ),
          headers: { 'Fiware-Service': service },
        });

        expect(nestedRows.status).toBe(200);
        expect(JSON.parse(nestedRows.json[0].device)).toEqual({
          name: 'sensor-x',
          meta: { floor: 3 },
        });
        expect(JSON.parse(nestedRows.json[0].readings)).toEqual([1, 2, 3]);

        // The same data projected with dot notation gets real, individually typed columns
        const createDotted = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Content-Type': 'application/json',
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: dottedFdaId,
            query: {
              collection: nestedCollectionName,
              filter: {},
              projection: {
                label: 1,
                'device.name': 1,
                'device.meta.floor': 1,
              },
            },
            description: 'mongo dotted projection fda',
            cached: true,
            datasourceId,
          },
        });

        expect(createDotted.status).toBe(202);
        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: dottedFdaId,
          visibility,
        });

        const dottedFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${dottedFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        const dottedSchema = Object.fromEntries(
          dottedFda.json.schema.map(({ name, type }) => [name, type]),
        );
        expect(dottedSchema['device.name']).toBe('VARCHAR');
        expect(dottedSchema['device.meta.floor']).toMatch(
          /^(BIGINT|INTEGER|DOUBLE)$/,
        );

        const dottedRows = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            dottedFdaId,
            'defaultDataAccess',
            { pageSize: 10, pageStart: 0 },
          ),
          headers: { 'Fiware-Service': service },
        });

        expect(dottedRows.status).toBe(200);
        expect(dottedRows.json).toHaveLength(1);
        expect(dottedRows.json[0].label).toBe('nested_doc');
        expect(dottedRows.json[0]['device.name']).toBe('sensor-x');
        expect(Number(dottedRows.json[0]['device.meta.floor'])).toBe(3);

        const dottedFiltered = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            dottedFdaId,
            'defaultDataAccess',
            { pageSize: 10, pageStart: 0, device_name: 'sensor-x' },
          ),
          headers: { 'Fiware-Service': service },
        });

        expect(dottedFiltered.status).toBe(200);
        expect(dottedFiltered.json).toHaveLength(1);
        expect(dottedFiltered.json[0]['device.name']).toBe('sensor-x');

        const dottedNoMatch = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            dottedFdaId,
            'defaultDataAccess',
            { pageSize: 10, pageStart: 0, device_name: 'sensor-y' },
          ),
          headers: { 'Fiware-Service': service },
        });

        expect(dottedNoMatch.status).toBe(200);
        expect(dottedNoMatch.json).toEqual([]);
      } finally {
        for (const id of [nestedFdaId, dottedFdaId]) {
          await httpReq({
            method: 'DELETE',
            url: `${baseUrl}/${visibility}/fdas/${id}`,
            headers: {
              'Fiware-Service': service,
              'Fiware-ServicePath': servicePath,
            },
          });
        }
      }
    });

    test('POST /fdas rejects disallowed aggregation stages', async () => {
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
          id: 'mongo_agg_out_not_allowed',
          query: {
            collection: collectionName,
            aggregation: [
              { $match: { site: 'lab' } },
              { $out: 'forbidden_target' },
            ],
          },
          description: 'mongo aggregation out not allowed',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain('stage $out is not allowed');
    });

    test('POST /fdas rejects cross-collection aggregation stages', async () => {
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
          id: 'mongo_agg_lookup_not_allowed',
          query: {
            collection: collectionName,
            aggregation: [
              { $match: { site: 'lab' } },
              {
                $lookup: {
                  from: 'otherCollection',
                  localField: 'site',
                  foreignField: 'site',
                  as: 'joined',
                },
              },
            ],
          },
          description: 'mongo aggregation lookup not allowed',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain('stage $lookup is not allowed');
    });

    test('POST /fdas rejects aggregation final $project without timeColumn', async () => {
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
          id: 'mongo_agg_timecolumn_missing',
          query: {
            collection: collectionName,
            aggregation: [
              { $match: { site: 'lab' } },
              { $project: { status: 1 } },
            ],
          },
          timeColumn: 'reading',
          description: 'mongo aggregation missing timeColumn in final project',
          cached: true,
          datasourceId,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidMongoFDAContract');
      expect(res.json.description).toContain(
        'timeColumn must be included in final aggregation $project stage',
      );
    });
  });

  describe('Mongo only-fresh FDAs', () => {
    const freshDatasourceId = 'mongo-fresh-ds';
    const freshFdaId = 'mongo_fresh_fda';
    const freshCollectionName = 'mongo_fresh_fda_events';

    beforeAll(async () => {
      const baseUrl = getBaseUrl();
      const mongoClient = new MongoClient(getMongoUri(), {
        serverSelectionTimeoutMS: 10_000,
      });

      await mongoClient.connect();
      try {
        const collection = mongoClient
          .db('test-db')
          .collection(freshCollectionName);
        await collection.deleteMany({});
        await collection.insertMany([
          { device: 'sensor-a', status: 'ok', reading: '21.5', site: 'lab' },
          { device: 'sensor-b', status: 'warn', reading: '19.2', site: 'lab' },
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
        url: `${baseUrl}/${visibility}/fdas/${freshFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/datasources/${freshDatasourceId}`,
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
          datasourceId: freshDatasourceId,
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
    });

    afterAll(async () => {
      const baseUrl = getBaseUrl();
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${freshFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    });

    test('POST /fdas supports cached=false for Mongo datasource', async () => {
      const baseUrl = getBaseUrl();

      const createFdaRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: freshFdaId,
          query: {
            collection: freshCollectionName,
            filter: { site: 'lab' },
            projection: {
              device: 1,
              status: 1,
              reading: 1,
            },
          },
          description: 'mongo only-fresh fda integration fixture',
          cached: false,
          datasourceId: freshDatasourceId,
        },
      });

      if (createFdaRes.status >= 400) {
        console.error(
          'Failed creating only-fresh Mongo FDA:',
          createFdaRes.status,
          createFdaRes.json,
        );
      }
      expect(createFdaRes.status).toBe(202);

      const getFdaRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${freshFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      expect(getFdaRes.status).toBe(200);
      expect(getFdaRes.json.cached).toBe(false);
    });

    test('GET /{fdaId}/data runs the only-fresh Mongo FDA directly against MongoDB', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'GET',
        url: buildFdaDataUrl(baseUrl, servicePath, freshFdaId),
        headers: { 'Fiware-Service': service },
      });

      if (res.status >= 400) {
        console.error(
          'Mongo only-fresh FDA JSON query failed:',
          res.status,
          res.json,
        );
      }

      expect(res.status).toBe(200);
      const sortedRows = [...res.json].sort((a, b) =>
        a.device.localeCompare(b.device),
      );
      expect(sortedRows).toEqual([
        { device: 'sensor-a', status: 'ok', reading: '21.5' },
        { device: 'sensor-b', status: 'warn', reading: '19.2' },
      ]);
    });

    test('GET /{fdaId}/data streams NDJSON for the only-fresh Mongo FDA', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReqRaw({
        method: 'GET',
        url: buildFdaDataUrl(baseUrl, servicePath, freshFdaId),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (res.status >= 400) {
        console.error(
          'Mongo only-fresh FDA NDJSON query failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(String(res.headers['content-type'])).toContain(
        'application/x-ndjson',
      );

      const rows = res.text
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line));

      expect(rows.map((row) => row.device).sort()).toEqual([
        'sensor-a',
        'sensor-b',
      ]);
    });

    test('POST /{fdaId}/das rejects DA creation for the only-fresh Mongo FDA', async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${freshFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: 'mongo_fresh_da_not_allowed',
          description: 'should be rejected',
          query: 'SELECT device WHERE status = $status',
          params: [{ name: 'status', type: 'Text', required: true }],
        },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('FDAOnlyFresh');
    });
  });
}
