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
import { MongoClient } from 'mongodb';

export function registerFdaCreationIntegrationTests({
  getBaseUrl,
  getMongoUri,
  getPgHost,
  getPgPort,
  service,
  servicePath,
  visibility,
  fdaId,
  fdaId3,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  test('POST /fdas creates an FDA (uploads CSV then converts to Parquet)', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId,
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
      },
    });

    if (res.status >= 400) {
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(202);
    const completedFDA = await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId,
    });

    expect(completedFDA.validationMode).toBe('strict');
    expect(Array.isArray(completedFDA.schema)).toBe(true);
    expect(completedFDA.schema.length).toBeGreaterThan(0);
  });

  test('POST /fdas creates an FDA and defaultDataAccess on an empty source query (includes timeColumn and partition case)', async () => {
    const baseUrl = getBaseUrl();
    const emptySourceFdaId = 'fda_empty_source_default_da';

    try {
      // Basic case
      const resBasic = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: emptySourceFdaId,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users WHERE 1 = 0 ORDER BY id',
          description: 'empty source default DA test',
          timeColumn: 'timeinstant',
        },
      });

      if (resBasic.status >= 400) {
        console.error(
          'POST /fdas failed for empty-source FDA:',
          resBasic.status,
          resBasic.json ?? resBasic.text,
        );
      }

      expect(resBasic.status).toBe(202);

      const completedFDABasic = await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: emptySourceFdaId,
      });

      expect(completedFDABasic.status).toBe('completed');
      const defaultDaBasic = completedFDABasic?.das?.defaultDataAccess;
      expect(defaultDaBasic).toBeDefined();
      expect(defaultDaBasic.query).toContain('COUNT(*) OVER() as __total');
      expect(defaultDaBasic.query).toContain('LIMIT CAST($pageSize AS BIGINT)');
      expect(defaultDaBasic.params.some((p) => p.name === 'pageSize')).toBe(
        true,
      );
      expect(defaultDaBasic.params.some((p) => p.name === 'pageStart')).toBe(
        true,
      );

      // Partitioned case
      const resPartitioned = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: `${emptySourceFdaId}_partitioned`,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users WHERE 1 = 0 ORDER BY id',
          description: 'empty source default DA test',
          timeColumn: 'timeinstant',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 day',
              fetchSize: 'year',
              windowSize: 'year',
            },
          },
          objStgConf: {
            partition: 'year',
          },
        },
      });

      if (resPartitioned.status >= 400) {
        console.error(
          'POST /fdas failed for empty-source FDA (partitioned):',
          resPartitioned.status,
          resPartitioned.json ?? resPartitioned.text,
        );
      }

      expect(resPartitioned.status).toBe(202);

      const completedFDAPartitioned = await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: `${emptySourceFdaId}_partitioned`,
      });

      const defaultDaPartitioned =
        completedFDAPartitioned?.das?.defaultDataAccess;
      expect(defaultDaPartitioned).toBeDefined();
      expect(defaultDaPartitioned.query).toContain(
        'COUNT(*) OVER() as __total',
      );
      expect(defaultDaPartitioned.query).toContain(
        'LIMIT CAST($pageSize AS BIGINT)',
      );
      expect(
        defaultDaPartitioned.params.some((p) => p.name === 'pageSize'),
      ).toBe(true);
      expect(
        defaultDaPartitioned.params.some((p) => p.name === 'pageStart'),
      ).toBe(true);
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${emptySourceFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${emptySourceFdaId}_partitioned`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });

  test('POST /fdas/:fdaId/das accepts numeric and datetime predicates when partitioned FDA schema is empty', async () => {
    const baseUrl = getBaseUrl();
    const fdaId = 'fda_empty_partitioned_typed_schema';
    const daId = 'da_empty_partitioned_typed_schema';

    try {
      const createFdaRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users WHERE 1 = 0 ORDER BY id',
          description: 'empty partitioned fda for typed-schema DA validation',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 day',
              fetchSize: 'day',
              windowSize: 'week',
            },
          },
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'day',
            compression: false,
          },
        },
      });

      expect(createFdaRes.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const createDaRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: daId,
          description: 'typed predicates over empty partitioned FDA',
          query: `
            SELECT id, age, timeinstant
            WHERE age >= $minAge
              AND CAST(timeinstant AS TIMESTAMP) >= CAST($start AS TIMESTAMP)
            ORDER BY id
          `,
          params: [
            { name: 'minAge', type: 'Number', required: true },
            { name: 'start', type: 'DateTime', required: true },
          ],
        },
      });

      if (createDaRes.status >= 400) {
        console.error(
          'POST /das failed for typed-schema empty partitioned FDA:',
          createDaRes.status,
          createDaRes.json ?? createDaRes.text,
        );
      }
      expect(createDaRes.status).toBe(204);

      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 0,
          start: '2020-01-01T00:00:00Z',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed for typed-schema empty partitioned FDA:',
          queryRes.status,
          queryRes.json ?? queryRes.text,
        );
      }

      expect(queryRes.status).toBe(200);
      expect(queryRes.json).toEqual([]);
    } finally {
      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    }
  });

  test('POST /fdas keeps one recurring refresh job per FDA', async () => {
    const baseUrl = getBaseUrl();
    const firstFdaId = 'fda_refresh_job_a';
    const secondFdaId = 'fda_refresh_job_b';
    const datasourceId = 'refresh-jobs-ds';

    const datasourceRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId,
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

    expect(datasourceRes.status).toBe(204);

    const createFda = async (id) => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id,
          query:
            'SELECT id, name, age, timeinstant FROM public.users ORDER BY id',
          description: `refresh job ${id}`,
          timeColumn: 'timeinstant',
          datasourceId,
          refreshPolicy: {
            type: 'interval',
            params: {
              refreshInterval: '1 hour',
            },
          },
        },
      });

      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: id });
    };

    await createFda(firstFdaId);
    await createFda(secondFdaId);

    const mongoClient = new MongoClient(getMongoUri(), {
      serverSelectionTimeoutMS: 10_000,
    });

    await mongoClient.connect();
    try {
      const collection = mongoClient.db().collection('agendaJobs');

      const expectedFilter = {
        name: 'refresh-fda-recurring',
        'data.service': service,
        'data.servicePath': servicePath,
        'data.fdaId': { $in: [firstFdaId, secondFdaId] },
      };

      const deadline = Date.now() + 10_000;
      let totalJobs = 0;
      while (Date.now() < deadline) {
        totalJobs = await collection.countDocuments(expectedFilter);
        if (totalJobs === 2) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }

      expect(totalJobs).toBe(2);
    } finally {
      await mongoClient.close();
    }
  });

  test('POST /fdas relaunches recurring jobs for multiple FDAs when nextRunAt is forced', async () => {
    const baseUrl = getBaseUrl();
    const firstFdaId = 'fda_relaunch_jobs_a';
    const secondFdaId = 'fda_relaunch_jobs_b';
    const datasourceId = 'relaunch-jobs-ds';

    const datasourceRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId,
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

    expect(datasourceRes.status).toBe(204);

    const createFda = async (id) => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id,
          query:
            'SELECT id, name, age, timeinstant FROM public.users ORDER BY id',
          description: `relaunch job ${id}`,
          timeColumn: 'timeinstant',
          datasourceId,
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 hour',
              fetchSize: 'day',
              windowSize: 'day',
            },
          },
          objStgConf: {
            partition: 'day',
          },
        },
      });

      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: id });
    };

    await createFda(firstFdaId);
    await createFda(secondFdaId);

    const mongoClient = new MongoClient(getMongoUri(), {
      serverSelectionTimeoutMS: 10_000,
    });

    await mongoClient.connect();
    try {
      const collection = mongoClient.db().collection('agendaJobs');
      const trackedNames = [
        'refresh-fda-recurring',
        'clean-partition-recurring',
      ];
      const trackedFdas = [firstFdaId, secondFdaId];

      const recurringFilter = {
        name: { $in: trackedNames },
        'data.service': service,
        'data.servicePath': servicePath,
        'data.fdaId': { $in: trackedFdas },
      };

      const waitForJobsDeadline = Date.now() + 15_000;
      let jobs = [];
      while (Date.now() < waitForJobsDeadline) {
        jobs = await collection
          .find(recurringFilter, {
            projection: {
              _id: 1,
              name: 1,
              'data.fdaId': 1,
              nextRunAt: 1,
              lastRunAt: 1,
              lastFinishedAt: 1,
            },
          })
          .toArray();

        if (jobs.length === 4) {
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 250));
      }

      expect(jobs).toHaveLength(4);

      const baseline = new Map(
        jobs.map((job) => {
          const key = `${job.name}:${job.data.fdaId}`;
          const value = job.lastRunAt ? new Date(job.lastRunAt).getTime() : -1;
          return [key, value];
        }),
      );

      await collection.updateMany(recurringFilter, {
        $set: {
          nextRunAt: new Date(Date.now() - 60_000),
          lockedAt: null,
        },
      });

      const executionDeadline = Date.now() + 40_000;
      let relaunched = false;

      while (Date.now() < executionDeadline) {
        const current = await collection
          .find(recurringFilter, {
            projection: {
              _id: 1,
              name: 1,
              'data.fdaId': 1,
              lastRunAt: 1,
              lastFinishedAt: 1,
            },
          })
          .toArray();

        if (current.length !== 4) {
          await new Promise((resolve) => setTimeout(resolve, 250));
          continue;
        }

        const allJobsExecuted = current.every((job) => {
          const key = `${job.name}:${job.data.fdaId}`;
          const previousLastRun = baseline.get(key) ?? -1;
          const currentLastRun = job.lastRunAt
            ? new Date(job.lastRunAt).getTime()
            : -1;

          return currentLastRun > previousLastRun && job.lastFinishedAt;
        });

        if (allJobsExecuted) {
          relaunched = true;
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 350));
      }

      expect(relaunched).toBe(true);
    } finally {
      await mongoClient.close();
    }
  });

  test('DELETE /fdas only removes the target FDA recurring jobs', async () => {
    const baseUrl = getBaseUrl();
    const firstFdaId = 'fda_delete_jobs_a';
    const secondFdaId = 'fda_delete_jobs_b';
    const datasourceId = 'delete-jobs-ds';

    const datasourceRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId,
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

    expect(datasourceRes.status).toBe(204);

    const createFda = async (id) => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id,
          query:
            'SELECT id, name, age, timeinstant FROM public.users ORDER BY id',
          description: `delete job ${id}`,
          timeColumn: 'timeinstant',
          datasourceId,
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '1 hour',
              fetchSize: 'day',
              windowSize: 'day',
            },
          },
          objStgConf: {
            partition: 'day',
          },
        },
      });

      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: id });
    };

    await createFda(firstFdaId);
    await createFda(secondFdaId);

    const mongoClient = new MongoClient(getMongoUri(), {
      serverSelectionTimeoutMS: 10_000,
    });

    await mongoClient.connect();
    try {
      const collection = mongoClient.db().collection('agendaJobs');
      const trackedNames = [
        'refresh-fda-recurring',
        'clean-partition-recurring',
      ];
      const recurringFilter = (fdaId) => ({
        name: { $in: trackedNames },
        'data.service': service,
        'data.servicePath': servicePath,
        'data.fdaId': fdaId,
      });

      const deadline = Date.now() + 10_000;
      let firstJobs = 0;
      let secondJobs = 0;
      while (Date.now() < deadline) {
        firstJobs = await collection.countDocuments(
          recurringFilter(firstFdaId),
        );
        secondJobs = await collection.countDocuments(
          recurringFilter(secondFdaId),
        );
        if (firstJobs === 2 && secondJobs === 2) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }

      expect(firstJobs).toBe(2);
      expect(secondJobs).toBe(2);

      const deleteRes = await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${firstFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      expect(deleteRes.status).toBe(204);

      const finalDeadline = Date.now() + 10_000;
      let remainingFirstJobs = 0;
      let remainingSecondJobs = 0;
      while (Date.now() < finalDeadline) {
        remainingFirstJobs = await collection.countDocuments(
          recurringFilter(firstFdaId),
        );
        remainingSecondJobs = await collection.countDocuments(
          recurringFilter(secondFdaId),
        );

        if (remainingFirstJobs === 0 && remainingSecondJobs === 2) {
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 250));
      }

      expect(remainingFirstJobs).toBe(0);
      expect(remainingSecondJobs).toBe(2);
    } finally {
      await mongoClient.close();
    }
  });

  test('POST /fdas tries to creates an FDA without id and is detected', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: null,
        // query base to extract from PG to CSV
        query: 'SELECT id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
      },
    });

    if (res.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(400);
  });

  test('POST /fdas try creates an FDA without body', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
    });
    if (res.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(400);
  });

  test('POST /fdas with duplicate id returns error', async () => {
    const baseUrl = getBaseUrl();

    await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: { 'Fiware-Service': service },
      body: {
        id: fdaId3,
        query: 'SELECT id FROM public.users',
        description: 'duplicate test',
      },
    });

    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId3, // same id
        query: 'SELECT id FROM public.users',
        description: 'duplicate test',
      },
    });

    expect(res.status).toBe(409);
    expect(res.json.error).toBe('DuplicatedKey');

    await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
  });

  test('POST /fdas allows same id in same service when servicePath differs', async () => {
    const baseUrl = getBaseUrl();
    const scopedFdaId = 'fda_same_id_diff_servicepath';
    const otherServicePath = '/other-path';

    const firstCreate = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: scopedFdaId,
        query: 'SELECT id FROM public.users',
        description: 'scope one',
      },
    });

    expect(firstCreate.status).toBe(202);

    const secondCreate = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': otherServicePath,
      },
      body: {
        id: scopedFdaId,
        query: 'SELECT id FROM public.users',
        description: 'scope two',
      },
    });

    expect(secondCreate.status).toBe(202);

    const wrongScopeRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': '/unknown-path',
      },
    });

    expect(wrongScopeRead.status).toBe(403);
    expect(wrongScopeRead.json.error).toBe('ServicePathMismatch');

    const scopeOneRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
    });

    expect(scopeOneRead.status).toBe(200);

    const scopeTwoRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': otherServicePath,
      },
    });

    expect(scopeTwoRead.status).toBe(200);

    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: scopedFdaId,
      servicePath,
    });
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: scopedFdaId,
      servicePath: otherServicePath,
    });
  });

  test('POST /fdas pending allows DA creation but rejects GET /{visibility}/fdas/{fdaId}/das/{daId}/data until first completion', async () => {
    const baseUrl = getBaseUrl();
    const pendingFdaId = 'fda_pending_first_fetch';
    const pendingDaId = 'da_pending_first_fetch';

    const createFda = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: pendingFdaId,
        // Force a long first fetch to keep the FDA non-queryable for this test.
        query:
          'SELECT id, name, age FROM public.users, (SELECT pg_sleep(6)) AS delayed_fetch',
        description: 'pending fda test',
      },
    });

    expect(createFda.status).toBe(202);

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${pendingFdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: pendingDaId,
        description: 'pending da test',
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

    expect(createDa.status).toBe(204);

    const queryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
        minAge: 20,
      }),
      headers: { 'Fiware-Service': service },
    });

    if (queryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected while FDA is pending:',
        queryRes.status,
        queryRes.json ?? queryRes.text,
      );
    }

    expect(queryRes.status).toBe(409);
    expect(queryRes.json.error).toBe('FDAUnavailable');

    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: pendingFdaId,
      timeout: 30000,
    });

    const queryAfterCompletion = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
        minAge: 20,
      }),
      headers: { 'Fiware-Service': service },
    });

    expect(queryAfterCompletion.status).toBe(200);
    expect(queryAfterCompletion.json).toEqual([
      { id: 1, name: 'ana', age: 30 },
      { id: 3, name: 'carlos', age: 40 },
    ]);
  });

  test('GET /fdas returns list', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status >= 400) {
      console.error('GET /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(200);
    expect(Array.isArray(res.json)).toBe(true);
    expect(res.json.some((x) => x.id === fdaId)).toBe(true);
  });
}
