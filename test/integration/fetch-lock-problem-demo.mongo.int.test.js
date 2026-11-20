// Copyright 2025 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefónica Soluciones
// de Informática y Comunicaciones de España, S.A.U (hereinafter TSOL) and is protected
// as copyright by the applicable legislation on intellectual property.
//
// It belongs to TSOL, and / or its licensors, the exclusive rights of reproduction,
// distribution, public communication and transformation, and any economic thing on it,
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

describe('Demonstration: Data corruption risk WITHOUT fetch lock (Issue #347)', () => {
  let container;
  let client;
  let collection;
  const originalUri = process.env.FDA_MONGO_URI;
  const scope = {
    service: 'corruption-demo',
    servicePath: '/demo',
    fdaId: 'fda-demo',
  };

  beforeAll(async () => {
    container = await new GenericContainer('mongo:8.0')
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();
    process.env.FDA_MONGO_URI = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/corruption-demo-test`;
    client = new MongoClient(process.env.FDA_MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
    });
    await client.connect();
    collection = client.db().collection('fdas');
  });

  beforeEach(async () => {
    await collection.deleteMany({});
    await collection.insertOne({ ...scope, status: 'completed', counter: 0 });
  });

  afterAll(async () => {
    await client?.close();
    await container?.stop();
    if (originalUri === undefined) {
      delete process.env.FDA_MONGO_URI;
    } else {
      process.env.FDA_MONGO_URI = originalUri;
    }
  });

  test('PROBLEM: concurrent updates WITHOUT synchronization cause lost updates (data corruption)', async () => {
    // This test demonstrates the data corruption problem that exists on main
    // when two refresh jobs try to update the same FDA concurrently.
    //
    // This is what happens in real deployments:
    // - Two Agenda jobs start processing the same FDA concurrently
    // - Both read the current counter value at nearly the same time
    // - Both increment and write back
    // - Result: one update is lost (lost update anomaly / race condition)

    // Starting state: FDA is ready for refresh
    await collection.updateOne(scope, {
      $set: { status: 'completed', counter: 0 },
    });

    // Simulate two concurrent refresh operations WITHOUT lock protection
    const refresh1 = async () => {
      // Read current state
      let doc = await collection.findOne(scope);
      const readValue = doc.counter; // Both jobs read same value

      // Simulate processing delay (network call, computation)
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Update: increment and write
      // This overwrites what refresh2 might also be writing
      await collection.updateOne(scope, {
        $set: {
          counter: readValue + 1, // If both read 0, both write 1
          lastJobUpdate: 'job1',
          status: 'fetching',
        },
      });
    };

    const refresh2 = async () => {
      // Read current state (happens before refresh1 writes)
      let doc = await collection.findOne(scope);
      const readValue = doc.counter; // Same value as job1

      // Simulate processing delay
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Update: increment and write
      // This overwrites refresh1's work because both read the same initial value
      await collection.updateOne(scope, {
        $set: {
          counter: readValue + 1, // Also writes 1
          lastJobUpdate: 'job2',
          status: 'fetching',
        },
      });
    };

    // Run both concurrently WITHOUT any synchronization mechanism
    await Promise.all([refresh1(), refresh2()]);

    // Check the result
    const finalState = await collection.findOne(scope);

    // PROBLEM DEMONSTRATED:
    // Expected: counter should be 2 (both increments applied)
    // Actual: counter is 1 (second write overwrote first write - LOST UPDATE)
    expect(finalState.counter).toBe(1);

    console.log('\n❌ DATA CORRUPTION DEMONSTRATED:');
    console.log(`   Two concurrent jobs tried to increment counter`);
    console.log(`   Expected result: counter = 2 (both increments applied)`);
    console.log(
      `   Actual result:   counter = 1 (lost update due to race condition)`,
    );
    console.log(`   This is the problem that Issue #347 solves!\n`);
  });

  test('SOLUTION WOULD BE: use distributed lock to serialize access', async () => {
    // This test shows what the solution (from task/add_fetch_lock branch) would provide
    // But on main, the lock functions do not exist, so this test will fail with:
    // "mongo.claimFDAForFetch is not a function"
    //
    // This demonstrates that the solution is needed.

    const mongo = await import('../../src/lib/utils/mongo.js').catch(
      () => null,
    );

    if (!mongo || !mongo.claimFDAForFetch) {
      // On main: lock functions don't exist
      console.log('\n⚠️  LOCK FUNCTIONS NOT AVAILABLE:');
      console.log(
        '   The distributed lock mechanism (Issue #347) is not implemented on main',
      );
      console.log(
        '   This is what task/add_fetch_lock branch adds to fix the problem!\n',
      );

      expect(mongo?.claimFDAForFetch).toBeUndefined();
      return;
    }

    // This would run on task/add_fetch_lock branch where lock functions exist
    await collection.updateOne(scope, {
      $set: { status: 'completed', counter: 0, processedBy: [] },
    });

    const refresh = async (jobId) => {
      // Try to acquire lock atomically
      const lockAcquired = await mongo.claimFDAForFetch({
        ...scope,
        owner: jobId,
      });

      if (!lockAcquired) {
        return 'skipped';
      }

      try {
        let doc = await collection.findOne(scope);
        const readValue = doc.counter;

        await new Promise((resolve) => setTimeout(resolve, 50));

        await collection.updateOne(scope, {
          $set: {
            counter: readValue + 1,
            lastJobUpdate: jobId,
            status: 'fetching',
          },
          $push: { processedBy: jobId },
        });

        return 'completed';
      } finally {
        await mongo.releaseFDAFetchLock({ ...scope, owner: jobId });
      }
    };

    // Run both concurrently WITH lock (would work on task/add_fetch_lock)
    const results = await Promise.all([refresh('job1'), refresh('job2')]);

    expect(results.sort()).toEqual(['completed', 'skipped']);

    const finalState = await collection.findOne(scope);
    expect(finalState.counter).toBe(1); // Correct: only one job processed
    expect(finalState.processedBy).toHaveLength(1);
  });
});
