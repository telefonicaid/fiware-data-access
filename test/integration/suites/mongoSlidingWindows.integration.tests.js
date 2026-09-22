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

export function registerMongoSlidingWindowsIntegrationTests({
  getBaseUrl,
  getMongoUri,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  waitForJobToFinish,
  buildDaDataUrl,
}) {
  const datasourceId = 'mongo-sliding-window-ds';

  async function withMongoClient(fn) {
    const mongoClient = new MongoClient(getMongoUri(), {
      serverSelectionTimeoutMS: 10_000,
    });
    await mongoClient.connect();
    try {
      return await fn(mongoClient.db('test-db'));
    } finally {
      await mongoClient.close();
    }
  }

  async function seedCollection(collectionName, docs) {
    await withMongoClient(async (db) => {
      const collection = db.collection(collectionName);
      await collection.deleteMany({});
      await collection.insertMany(docs);
    });
  }

  async function insertDoc(collectionName, doc) {
    await withMongoClient((db) => db.collection(collectionName).insertOne(doc));
  }

  function readDefaultDA(baseUrl, fdaId) {
    return httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, 'defaultDataAccess', {
        pageSize: 100,
        pageStart: 0,
      }),
      headers: { 'Fiware-Service': service },
    });
  }

  function readDaData(baseUrl, fdaId, daId, params = {}) {
    return httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, params),
      headers: { 'Fiware-Service': service },
    });
  }

  function createDA(baseUrl, fdaId, daId, query, params) {
    return httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: { id: daId, description: daId, query, params },
    });
  }

  describe('Mongo sliding window FDAs', () => {
    beforeAll(async () => {
      const baseUrl = getBaseUrl();

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
    });

    test('POST /fdas accepts a Mongo filter query with window refresh policy and keeps only in-window rows', async () => {
      const baseUrl = getBaseUrl();
      const suffix = `${Date.now()}`;
      const collectionName = `mongo_sw_filter_${suffix}`;
      const fdaId = `fda_mongo_sw_filter_${suffix}`;
      const now = Date.now();

      await seedCollection(collectionName, [
        {
          label: 'outside_window_old',
          observedAt: new Date(now - 8 * 24 * 60 * 60 * 1000),
        },
        {
          label: 'inside_window_20h',
          observedAt: new Date(now - 20 * 60 * 60 * 1000),
        },
        {
          label: 'inside_window_2h',
          observedAt: new Date(now - 2 * 60 * 60 * 1000),
        },
      ]);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          datasourceId,
          query: {
            collection: collectionName,
            filter: {},
            projection: { label: 1, observedAt: 1 },
          },
          description: 'Mongo filter sliding window test',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 hour',
              fetchSize: 'day',
              windowSize: 'week',
            },
          },
          objStgConf: { partition: 'day' },
          timeColumn: 'observedAt',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const readRes = await readDefaultDA(baseUrl, fdaId);

      expect(readRes.status).toBe(200);
      expect(new Set(readRes.json.map((r) => r.label))).toEqual(
        new Set(['inside_window_2h', 'inside_window_20h']),
      );
    });

    test('POST /fdas accepts a Mongo aggregation query with window refresh policy, filtering before the pipeline runs', async () => {
      const baseUrl = getBaseUrl();
      const suffix = `${Date.now()}`;
      const collectionName = `mongo_sw_agg_${suffix}`;
      const fdaId = `fda_mongo_sw_agg_${suffix}`;
      const now = Date.now();

      await seedCollection(collectionName, [
        {
          label: 'outside_window_old',
          site: 'lab',
          observedAt: new Date(now - 8 * 24 * 60 * 60 * 1000),
        },
        {
          label: 'inside_window_lab',
          site: 'lab',
          observedAt: new Date(now - 60 * 60 * 1000),
        },
        {
          label: 'inside_window_other_site',
          site: 'remote',
          observedAt: new Date(now - 60 * 60 * 1000),
        },
      ]);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          datasourceId,
          query: {
            collection: collectionName,
            aggregation: [
              { $match: { site: 'lab' } },
              { $project: { label: 1, site: 1, observedAt: 1 } },
            ],
          },
          description: 'Mongo aggregation sliding window test',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 hour',
              fetchSize: 'day',
              windowSize: 'week',
            },
          },
          objStgConf: { partition: 'day' },
          timeColumn: 'observedAt',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const readRes = await readDefaultDA(baseUrl, fdaId);

      expect(readRes.status).toBe(200);
      // The window $match runs before the user's own $match on `site`, but the
      // end result still only contains rows matching both conditions.
      expect(readRes.json.map((r) => r.label)).toEqual(['inside_window_lab']);
    });

    test('recurring Mongo refresh job picks up rows inserted after creation', async () => {
      const baseUrl = getBaseUrl();
      const suffix = `${Date.now()}`;
      const collectionName = `mongo_sw_recurring_${suffix}`;
      const fdaId = `fda_mongo_sw_recurring_${suffix}`;
      const now = Date.now();

      await seedCollection(collectionName, [
        { label: 'initial', observedAt: new Date(now - 30 * 60 * 1000) },
      ]);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          datasourceId,
          query: {
            collection: collectionName,
            filter: {},
            projection: { label: 1, observedAt: 1 },
          },
          description: 'Mongo recurring refresh test',
          refreshPolicy: {
            type: 'window',
            params: { refreshInterval: '1 hour', fetchSize: 'hour' },
          },
          timeColumn: 'observedAt',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      let readRes = await readDefaultDA(baseUrl, fdaId);
      expect(new Set(readRes.json.map((r) => r.label))).toEqual(
        new Set(['initial']),
      );

      // Insert a new row, then force the recurring job to run right now instead of
      // waiting for refreshInterval: its upper time bound ($$NOW) is resolved by
      // MongoDB at this execution, so the new row must be picked up.
      await insertDoc(collectionName, {
        label: 'added_after_creation',
        observedAt: new Date(),
      });

      await withMongoClient(async (db) => {
        const agendaJobs = db.collection('agendaJobs');
        const updateResult = await agendaJobs.updateOne(
          { name: 'refresh-fda-recurring', 'data.fdaId': fdaId },
          { $set: { nextRunAt: new Date() } },
        );
        expect(updateResult.modifiedCount).toBe(1);
        await waitForJobToFinish(agendaJobs, fdaId, 'refresh-fda-recurring');
      });

      readRes = await readDefaultDA(baseUrl, fdaId);
      expect(new Set(readRes.json.map((r) => r.label))).toEqual(
        new Set(['initial', 'added_after_creation']),
      );
    });

    test('schedules clean-partition-recurring alongside refresh-fda-recurring for a partitioned Mongo FDA', async () => {
      const baseUrl = getBaseUrl();
      const suffix = `${Date.now()}`;
      const collectionName = `mongo_sw_clean_sched_${suffix}`;
      const fdaId = `fda_mongo_sw_clean_sched_${suffix}`;

      await seedCollection(collectionName, [
        { label: 'seed', observedAt: new Date() },
      ]);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          datasourceId,
          query: {
            collection: collectionName,
            filter: {},
            projection: { label: 1, observedAt: 1 },
          },
          description: 'Mongo clean-partition scheduling test',
          timeColumn: 'observedAt',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 hour',
              fetchSize: 'day',
              windowSize: 'day',
            },
          },
          objStgConf: { partition: 'day' },
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      await withMongoClient(async (db) => {
        const agendaJobs = db.collection('agendaJobs');
        const trackedNames = [
          'refresh-fda-recurring',
          'clean-partition-recurring',
        ];

        const waitForJobsDeadline = Date.now() + 15_000;
        let jobs = [];
        while (Date.now() < waitForJobsDeadline) {
          jobs = await agendaJobs
            .find({
              name: { $in: trackedNames },
              'data.service': service,
              'data.servicePath': servicePath,
              'data.fdaId': fdaId,
            })
            .toArray();

          if (jobs.length === trackedNames.length) {
            break;
          }

          await new Promise((resolve) => setTimeout(resolve, 250));
        }

        expect(jobs.map((job) => job.name).sort()).toEqual(
          [...trackedNames].sort(),
        );
      });
    });

    test('PUT /fdas/:fdaId regenerates a Mongo sliding window FDA keeping only current window rows', async () => {
      const baseUrl = getBaseUrl();
      const suffix = `${Date.now()}`;
      const collectionName = `mongo_sw_put_${suffix}`;
      const fdaId = `fda_mongo_sw_put_${suffix}`;
      const now = Date.now();

      await seedCollection(collectionName, [
        {
          label: 'old_before_create',
          observedAt: new Date(now - 10 * 24 * 60 * 60 * 1000),
        },
        {
          label: 'recent_before_create',
          observedAt: new Date(now - 2 * 60 * 60 * 1000),
        },
      ]);

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          datasourceId,
          query: {
            collection: collectionName,
            filter: {},
            projection: { label: 1, observedAt: 1 },
          },
          description: 'Mongo PUT regenerate sliding window test',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '6 hours',
              fetchSize: 'day',
              windowSize: 'week',
            },
          },
          timeColumn: 'observedAt',
          objStgConf: { partition: 'day' },
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const firstRead = await readDefaultDA(baseUrl, fdaId);
      expect(firstRead.status).toBe(200);
      expect(firstRead.json.map((row) => row.label)).toEqual([
        'recent_before_create',
      ]);

      await insertDoc(collectionName, {
        label: 'recent_before_update',
        observedAt: new Date(now - 60 * 60 * 1000),
      });
      await insertDoc(collectionName, {
        label: 'old_before_update',
        observedAt: new Date(now - 9 * 24 * 60 * 60 * 1000),
      });

      const updateFda = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      expect(updateFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const secondRead = await readDefaultDA(baseUrl, fdaId);
      expect(secondRead.status).toBe(200);
      expect(new Set(secondRead.json.map((row) => row.label))).toEqual(
        new Set(['recent_before_create', 'recent_before_update']),
      );
    });

    describe('DA filters on non-string Mongo field types', () => {
      let fdaId;
      let collectionName;

      beforeAll(async () => {
        const baseUrl = getBaseUrl();
        const suffix = `${Date.now()}`;
        collectionName = `mongo_sw_types_${suffix}`;
        fdaId = `fda_mongo_sw_types_${suffix}`;
        const now = Date.now();

        await seedCollection(collectionName, [
          {
            label: 'a',
            temperature: 21.5,
            active: true,
            observedAt: new Date(now - 60 * 60 * 1000),
          },
          {
            label: 'b',
            temperature: 19.2,
            active: false,
            observedAt: new Date(now - 60 * 60 * 1000),
          },
          {
            label: 'c',
            temperature: 21.5,
            active: true,
            observedAt: new Date(now - 60 * 60 * 1000),
          },
        ]);

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            datasourceId,
            query: {
              collection: collectionName,
              filter: {},
              projection: {
                label: 1,
                temperature: 1,
                active: 1,
                observedAt: 1,
              },
            },
            description: 'Mongo non-string field types test',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '1 hour',
                fetchSize: 'day',
                windowSize: 'week',
              },
            },
            objStgConf: { partition: 'day' },
            timeColumn: 'observedAt',
          },
        });

        if (createFda.status >= 400) {
          throw new Error(
            `Failed to create Mongo FDA: ${createFda.status} ${JSON.stringify(createFda.json)}`,
          );
        }
        await waitUntilFDACompleted({ baseUrl, service, fdaId });
      });

      test('the persisted schema reports the real types of the materialized Parquet', async () => {
        const baseUrl = getBaseUrl();
        const getFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(getFda.status).toBe(200);

        const schemaByName = Object.fromEntries(
          getFda.json.schema.map(({ name, type }) => [name, type]),
        );

        expect(Object.keys(schemaByName).sort()).toEqual([
          'active',
          'label',
          'observedAt',
          'temperature',
        ]);
        expect(schemaByName.label).toBe('VARCHAR');
        expect(schemaByName.active).toBe('BOOLEAN');
        expect(schemaByName.temperature).toMatch(
          /^(DOUBLE|FLOAT|DECIMAL.*|BIGINT|INTEGER)$/,
        );
        expect(schemaByName.observedAt).toMatch(/^TIMESTAMP/);
      });

      test('defaultDataAccess equality filter works on a genuinely numeric Mongo field', async () => {
        const baseUrl = getBaseUrl();
        const readRes = await readDaData(baseUrl, fdaId, 'defaultDataAccess', {
          pageSize: 100,
          pageStart: 0,
          temperature: '21.5',
        });

        expect(readRes.status).toBe(200);
        expect(new Set(readRes.json.map((r) => r.label))).toEqual(
          new Set(['a', 'c']),
        );
      });

      test('defaultDataAccess equality filter works on a genuinely boolean Mongo field', async () => {
        const baseUrl = getBaseUrl();
        const readRes = await readDaData(baseUrl, fdaId, 'defaultDataAccess', {
          pageSize: 100,
          pageStart: 0,
          active: 'true',
        });

        expect(readRes.status).toBe(200);
        expect(new Set(readRes.json.map((r) => r.label))).toEqual(
          new Set(['a', 'c']),
        );
      });

      test('custom DA: an uncast numeric comparison against a Mongo-sourced field works end to end', async () => {
        const baseUrl = getBaseUrl();
        const daId = 'numericNoCast';

        const createRes = await createDA(
          baseUrl,
          fdaId,
          daId,
          'SELECT * WHERE "temperature" > $minTemp',
          [{ name: 'minTemp', type: 'Number', required: true }],
        );
        expect(createRes.status).toBe(204);

        const readRes = await readDaData(baseUrl, fdaId, daId, {
          pageSize: 100,
          pageStart: 0,
          minTemp: 20,
        });

        expect(readRes.status).toBe(200);
        expect(new Set(readRes.json.map((r) => r.label))).toEqual(
          new Set(['a', 'c']),
        );
      });

      test('custom DA: an explicit CAST makes numeric comparisons reliable on a Mongo-sourced field', async () => {
        const baseUrl = getBaseUrl();
        const daId = 'numericWithCast';

        const createRes = await createDA(
          baseUrl,
          fdaId,
          daId,
          'SELECT * WHERE CAST("temperature" AS DOUBLE) > $minTemp',
          [{ name: 'minTemp', type: 'Number', required: true }],
        );

        expect(createRes.status).toBe(204);

        const readRes = await readDaData(baseUrl, fdaId, daId, {
          pageSize: 100,
          pageStart: 0,
          minTemp: 20,
        });

        expect(readRes.status).toBe(200);
        expect(new Set(readRes.json.map((r) => r.label))).toEqual(
          new Set(['a', 'c']),
        );
      });

      test('a field with heterogeneous types across documents still materializes, widened to text (see issue #235)', async () => {
        const baseUrl = getBaseUrl();
        const suffix = `${Date.now()}`;
        const heteroCollectionName = `mongo_sw_hetero_${suffix}`;
        const heteroFdaId = `fda_mongo_sw_hetero_${suffix}`;
        const now = Date.now();

        await seedCollection(heteroCollectionName, [
          {
            label: 'numeric_doc',
            temperature: 25,
            observedAt: new Date(now - 60 * 60 * 1000),
          },
          {
            label: 'string_doc',
            temperature: 'high',
            observedAt: new Date(now - 60 * 60 * 1000),
          },
        ]);

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: heteroFdaId,
            datasourceId,
            query: {
              collection: heteroCollectionName,
              filter: {},
              projection: { label: 1, temperature: 1, observedAt: 1 },
            },
            description: 'Mongo heterogeneous field type test',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '1 hour',
                fetchSize: 'day',
                windowSize: 'week',
              },
            },
            objStgConf: { partition: 'day' },
            timeColumn: 'observedAt',
          },
        });

        // Ingestion must not crash just because the same field has mixed types across documents
        expect(createFda.status).toBe(202);
        await waitUntilFDACompleted({ baseUrl, service, fdaId: heteroFdaId });

        const readRes = await readDefaultDA(baseUrl, heteroFdaId);
        expect(readRes.status).toBe(200);
        expect(new Set(readRes.json.map((r) => r.label))).toEqual(
          new Set(['numeric_doc', 'string_doc']),
        );

        // The actual Parquet column is typed as text, so both numeric and string values are preserved
        const temperatures = readRes.json.map((r) => r.temperature);
        expect(temperatures.sort()).toEqual(['25', 'high']);
      });

      test('a field missing from some documents is still materialized for every document', async () => {
        const baseUrl = getBaseUrl();
        const suffix = `${Date.now()}`;
        const sparseCollectionName = `mongo_sw_sparse_${suffix}`;
        const sparseFdaId = `fda_mongo_sw_sparse_${suffix}`;
        const now = Date.now();

        // The first document lacks `extra`. The declared projection is the column contract instead
        await seedCollection(sparseCollectionName, [
          {
            label: 'without_extra',
            observedAt: new Date(now - 60 * 60 * 1000),
          },
          {
            label: 'with_extra',
            extra: 'present',
            observedAt: new Date(now - 30 * 60 * 1000),
          },
        ]);

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: sparseFdaId,
            datasourceId,
            query: {
              collection: sparseCollectionName,
              filter: {},
              projection: { label: 1, extra: 1, observedAt: 1 },
            },
            description: 'Mongo heterogeneous field set test',
            timeColumn: 'observedAt',
          },
        });

        expect(createFda.status).toBe(202);
        await waitUntilFDACompleted({ baseUrl, service, fdaId: sparseFdaId });

        const getFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${sparseFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(getFda.status).toBe(200);
        expect(getFda.json.schema.map(({ name }) => name).sort()).toEqual([
          'extra',
          'label',
          'observedAt',
        ]);

        const readRes = await readDefaultDA(baseUrl, sparseFdaId);
        expect(readRes.status).toBe(200);

        const extraByLabel = Object.fromEntries(
          readRes.json.map((row) => [row.label, row.extra]),
        );
        expect(extraByLabel.with_extra).toBe('present');
        expect(extraByLabel.without_extra ?? null).toBeNull();
      });
    });
  });
}
