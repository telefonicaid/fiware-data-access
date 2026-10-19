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

describe('FDA refresh lock with MongoDB', () => {
  let container;
  let client;
  let mongo;
  let collection;
  const originalUri = process.env.FDA_MONGO_URI;
  const scope = {
    service: 'refresh-lock',
    servicePath: '/integration',
    fdaId: 'fda1',
  };

  beforeAll(async () => {
    container = await new GenericContainer('mongo:8.0')
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();
    process.env.FDA_MONGO_URI = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/refresh-lock-test`;
    client = new MongoClient(process.env.FDA_MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
    });
    await client.connect();
    collection = client.db().collection('fdas');
    mongo = await import('../../src/lib/utils/mongo.js');
  });

  beforeEach(async () => {
    await collection.deleteMany({});
    await collection.insertOne({ ...scope, status: 'completed' });
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

  test('atomically allows only one concurrent refresh for the same FDA', async () => {
    const claims = await Promise.all(
      ['owner-1', 'owner-2'].map((owner) =>
        mongo.claimFDAForFetch({ ...scope, owner }),
      ),
    );

    expect(claims.filter(Boolean)).toHaveLength(1);
    expect(claims.filter(Boolean)[0]).toBe(true);
    expect(
      await collection.countDocuments({
        ...scope,
        refreshLock: { $exists: true },
      }),
    ).toBe(1);
  });

  test('does not let a different owner renew or release the lock', async () => {
    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);

    await expect(
      mongo.renewFDAFetchLock({ ...scope, owner: 'owner-2' }),
    ).resolves.toBe(false);
    await expect(
      mongo.releaseFDAFetchLock({ ...scope, owner: 'owner-2' }),
    ).resolves.toBe(false);
    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-2' }),
    ).resolves.toBe(false);

    await expect(
      mongo.releaseFDAFetchLock({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);
  });

  test('renews an active lease and permits acquisition after expiration', async () => {
    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);
    await expect(
      mongo.renewFDAFetchLock({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);

    await collection.updateOne(scope, {
      $set: { 'refreshLock.expiresAt': new Date(Date.now() - 1000) },
    });

    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-2' }),
    ).resolves.toBe(true);
    const fda = await collection.findOne(scope);
    expect(fda.refreshLock.owner).toBe('owner-2');
  });

  test('keeps locks independent across FDA scopes', async () => {
    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);
    await collection.insertOne({
      ...scope,
      fdaId: 'fda2',
      status: 'completed',
    });

    await expect(
      mongo.claimFDAForFetch({
        ...scope,
        fdaId: 'fda2',
        owner: 'owner-2',
      }),
    ).resolves.toBe(true);
  });

  test('prevents deletion while locked and allows it after the lease expires', async () => {
    await expect(
      mongo.claimFDAForFetch({ ...scope, owner: 'owner-1' }),
    ).resolves.toBe(true);
    await expect(
      mongo.claimFDAForDeletion(scope.service, scope.fdaId, scope.servicePath),
    ).resolves.toBeNull();

    await collection.updateOne(scope, {
      $set: { 'refreshLock.expiresAt': new Date(Date.now() - 1000) },
    });

    await expect(
      mongo.claimFDAForDeletion(scope.service, scope.fdaId, scope.servicePath),
    ).resolves.toMatchObject({ status: 'fetching' });
  });
});
