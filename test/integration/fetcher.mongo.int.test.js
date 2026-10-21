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

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import { GenericContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';

jest.setTimeout(120000);

describe('Recurring refresh consistency check with Agenda and MongoDB', () => {
  let container;
  let client;
  let agenda;
  let collection;
  const scope = {
    service: 'svcA',
    servicePath: '/public',
    fdaId: 'fdaA',
  };
  const processFDAAsync = jest.fn().mockResolvedValue(undefined);
  const logger = {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };

  beforeAll(async () => {
    container = await new GenericContainer('mongo:8.0')
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();
    const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/fetcher-test`;
    client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000 });
    await client.connect();
    collection = client.db().collection('agendaJobs');

    await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
      config: {
        mongo: { uri },
        fetcher: { maxConcurrentRefreshJobs: 1, heartbeatIntervalMs: 0 },
      },
    }));
    await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
      processFDAAsync,
      cleanPartition: jest.fn(),
      processUploadFDAJob: jest.fn(),
    }));
    await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
      getBasicLogger: () => logger,
      createChildLogger: () => logger,
      runWithLogger: (jobLogger, callback) => callback(),
    }));

    const { initAgenda } = await import('../../src/lib/jobs.js');
    agenda = initAgenda();
    const { startFetcher } = await import('../../src/fetcher.js');
    await startFetcher();
  });

  beforeEach(async () => {
    await collection.deleteMany({});
    jest.clearAllMocks();
  });

  afterAll(async () => {
    await agenda?.stop(true);
    await client?.close();
    await container?.stop();
  });

  function insertConsistencyJob(data = scope, lockedAt = new Date()) {
    return collection.insertOne({
      name: 'consistency-refresh-fda-recurring',
      data,
      lockedAt,
      nextRunAt: null,
    });
  }

  async function runRecurringRefresh() {
    const job = agenda
      .create('refresh-fda-recurring', { ...scope, query: 'SELECT 1' })
      .schedule(new Date(Date.now() + 3600000));
    await job.save();
    await job.run();
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
  }

  test('skips recurring refresh while the same FDA has a locked consistency job', async () => {
    await insertConsistencyJob();

    await runRecurringRefresh();

    expect(processFDAAsync).not.toHaveBeenCalled();
    expect(logger.info).toHaveBeenCalledWith(
      { fdaId: scope.fdaId },
      'Skipping recurring refresh because consistency refresh is in progress',
    );
  });

  test('resumes recurring refresh after the consistency job is unlocked', async () => {
    const { insertedId } = await insertConsistencyJob();
    await runRecurringRefresh();
    expect(processFDAAsync).not.toHaveBeenCalled();

    await collection.updateOne(
      { _id: insertedId },
      { $unset: { lockedAt: true } },
    );
    await runRecurringRefresh();

    expect(processFDAAsync).toHaveBeenCalledTimes(1);
  });

  test('runs recurring refresh when no consistency job exists', async () => {
    await runRecurringRefresh();

    expect(processFDAAsync).toHaveBeenCalledTimes(1);
  });

  test('runs recurring refresh when the consistency job has a null lock', async () => {
    await insertConsistencyJob(scope, null);

    await runRecurringRefresh();

    expect(processFDAAsync).toHaveBeenCalledTimes(1);
  });

  test.each([
    ['service', 'other-service'],
    ['servicePath', '/other'],
    ['fdaId', 'other-fda'],
  ])(
    'ignores a locked consistency job with a different %s',
    async (field, value) => {
      await insertConsistencyJob({ ...scope, [field]: value });

      await runRecurringRefresh();

      expect(processFDAAsync).toHaveBeenCalledTimes(1);
    },
  );
});
