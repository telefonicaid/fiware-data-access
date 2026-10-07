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
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import { GenericContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';

jest.setTimeout(120000);

describe('Accounting persistence', () => {
  let container;
  let client;
  let mongo;
  const originalUri = process.env.FDA_MONGO_URI;
  const scope = { service: 'accounting', servicePath: '/a', fdaId: 'fda1' };
  const oldFetch = new Date('2026-10-01T00:00:00Z');
  const entry = {
    ...scope,
    trackerId: 'tracker1',
    sequence: 1,
    lastFetch: oldFetch,
    count: 2,
    lastAccessAt: new Date('2026-10-02T00:00:00Z'),
    das: { da1: { count: 2, lastAccessAt: new Date('2026-10-02T00:00:00Z') } },
  };

  beforeAll(async () => {
    container = await new GenericContainer('mongo:8.0')
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();
    process.env.FDA_MONGO_URI = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/accounting-test`;
    client = new MongoClient(process.env.FDA_MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
    });
    await client.connect();
    mongo = await import('../../src/lib/utils/mongo.js');
    await client
      .db()
      .collection('fdas')
      .insertOne({
        ...scope,
        lastFetch: oldFetch,
        das: { da1: { query: 'SELECT *' } },
      });
  });

  afterAll(async () => {
    await mongo?.disconnectClient();
    await client?.close();
    await container?.stop();
    if (originalUri === undefined) {
      delete process.env.FDA_MONGO_URI;
    } else {
      process.env.FDA_MONGO_URI = originalUri;
    }
  });

  test('retries an acknowledged or partially applied batch without double counting', async () => {
    await mongo.recordFDAAccesses([entry]);
    await mongo.recordFDAAccesses([entry]);
    const fda = await client.db().collection('fdas').findOne(scope);
    expect(fda.access).toMatchObject({ count: 2, sinceLastFetch: 2 });
    expect(fda.das.da1.access).toMatchObject({ count: 2, sinceLastFetch: 2 });
  });

  test('counts late old-generation queries only in lifetime totals', async () => {
    await mongo.updateFDAStatus({
      ...scope,
      status: 'completed',
      progress: 100,
      lastRefresh: { durationMs: 123, bytesFetched: 456 },
    });
    await mongo.recordFDAAccesses([{ ...entry, sequence: 2 }]);
    const fda = await client.db().collection('fdas').findOne(scope);
    expect(fda.access).toMatchObject({ count: 4, sinceLastFetch: 0 });
    expect(fda.das.da1.access).toMatchObject({ count: 4, sinceLastFetch: 0 });
    expect(fda.lastRefresh).toEqual({ durationMs: 123, bytesFetched: 456 });
  });

  test('counts the new generation and does not recreate deleted DAs', async () => {
    const collection = client.db().collection('fdas');
    const previous = await collection.findOne(scope);
    await mongo.recordFDAAccesses([
      {
        ...entry,
        sequence: 3,
        lastFetch: previous.lastFetch,
        count: 1,
        das: {
          da1: { count: 1, lastAccessAt: entry.lastAccessAt },
          deleted: { count: 1, lastAccessAt: entry.lastAccessAt },
        },
      },
    ]);
    const fda = await collection.findOne(scope);
    expect(fda.access).toMatchObject({ count: 5, sinceLastFetch: 1 });
    expect(fda.das.da1.access).toMatchObject({ count: 5, sinceLastFetch: 1 });
    expect(fda.das.deleted).toBeUndefined();
  });

  test('isolates tenants and exposes persisted figures in both aggregation paths', async () => {
    await mongo.updateFDAStorage(
      scope.service,
      scope.fdaId,
      scope.servicePath,
      { bytes: 10, objects: 2, partitions: 1, measuredAt: new Date() },
    );
    await client
      .db()
      .collection('fdas')
      .insertOne({ service: 'other', fdaId: 'fda1', storage: { bytes: 999 } });
    const groups = await mongo.aggregateFDAUsage(scope.service);
    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      servicePath: '/a',
      fdas: 1,
      bytes: 10,
      objects: 2,
      partitions: 1,
      queries: 5,
      sinceLastFetch: 1,
      refreshDurationMs: 123,
      refreshBytesFetched: 456,
    });
    const snapshot = await mongo.getOperationalCollectionsSnapshot();
    expect(
      snapshot.fdasByServiceAndPath.find(
        (group) => group.service === scope.service,
      ),
    ).toMatchObject({ bytes: 10, queries: 5 });
  });
});
