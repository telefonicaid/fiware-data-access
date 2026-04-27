// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefonica Soluciones
// de Informatica y Comunicaciones de Espana, S.A.U (hereinafter TSOL) and is protected
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

import { beforeEach, describe, expect, jest, test } from '@jest/globals';

const loggerMock = {
  debug: jest.fn(),
};

const mongoClientCtorMock = jest.fn();

let collectionMock;
let dbMock;
let clientMock;

async function loadMongoModule({ connectError } = {}) {
  jest.resetModules();

  loggerMock.debug.mockReset();
  mongoClientCtorMock.mockReset();

  collectionMock = {
    dropIndex: jest.fn().mockResolvedValue(undefined),
    createIndex: jest.fn(),
    insertOne: jest.fn().mockResolvedValue(undefined),
    updateOne: jest.fn().mockResolvedValue(undefined),
    findOneAndUpdate: jest.fn().mockResolvedValue({ id: 'prev' }),
    findOne: jest.fn().mockResolvedValue({ status: 'completed' }),
    deleteOne: jest.fn().mockResolvedValue({ deletedCount: 1 }),
    aggregate: jest.fn().mockReturnValue({
      toArray: jest.fn().mockResolvedValue([{ das: [] }]),
    }),
    find: jest.fn().mockReturnValue({
      toArray: jest.fn().mockResolvedValue([]),
    }),
  };

  dbMock = {
    databaseName: 'test-db',
    collection: jest.fn(() => collectionMock),
  };

  clientMock = {
    connect: connectError
      ? jest.fn().mockRejectedValue(connectError)
      : jest.fn().mockResolvedValue(undefined),
    db: jest.fn(() => dbMock),
    close: jest.fn().mockResolvedValue(undefined),
  };

  mongoClientCtorMock.mockImplementation(() => clientMock);

  await jest.unstable_mockModule('mongodb', () => ({
    MongoClient: mongoClientCtorMock,
  }));

  await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
    config: {
      mongo: {
        uri: 'mongodb://mongo:27017/test-db',
      },
    },
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  const mod = await import('../../src/lib/utils/mongo.js');
  return { ...mod, collectionMock, clientMock };
}

describe('mongo utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('createIndex wraps connection failures as MongoConnectionError', async () => {
    const { createIndex } = await loadMongoModule({
      connectError: new Error('cannot connect'),
    });

    await expect(createIndex()).rejects.toMatchObject({
      status: 503,
      type: 'MongoConnectionError',
    });
  });

  test('createFDAMongo wraps unexpected insert errors', async () => {
    const { createFDAMongo, collectionMock } = await loadMongoModule();

    collectionMock.insertOne.mockRejectedValueOnce(new Error('insert fail'));

    await expect(
      createFDAMongo('fdaA', 'SELECT 1', 'svc', 'public', '/sp', 'desc', {
        type: 'none',
      }),
    ).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('createFDAMongo marks only-fresh FDAs as completed immediately', async () => {
    const { createFDAMongo, collectionMock } = await loadMongoModule();

    await createFDAMongo(
      'fdaA',
      'SELECT 1',
      'svc',
      'public',
      '/sp',
      'desc',
      { type: 'none' },
      undefined,
      undefined,
      false,
    );

    expect(collectionMock.insertOne).toHaveBeenCalledWith(
      expect.objectContaining({
        fdaId: 'fdaA',
        cached: false,
        status: 'completed',
        progress: 100,
        lastFetch: null,
      }),
    );
  });

  test('updateFDAStatus includes optional error in update payload', async () => {
    const { updateFDAStatus, collectionMock } = await loadMongoModule();

    await updateFDAStatus('svc', 'fdaA', '/sp', 'failed', 0, 'boom');

    expect(collectionMock.updateOne).toHaveBeenCalledWith(
      { service: 'svc', fdaId: 'fdaA', servicePath: '/sp' },
      {
        $set: expect.objectContaining({
          status: 'failed',
          progress: 0,
          error: 'boom',
        }),
      },
    );
  });

  test('regenerateFDA throws NotFound when FDA does not exist', async () => {
    const { regenerateFDA, collectionMock } = await loadMongoModule();

    collectionMock.findOneAndUpdate.mockResolvedValueOnce(null);
    collectionMock.findOne.mockResolvedValueOnce(null);

    await expect(regenerateFDA('svc', 'missing', '/sp')).rejects.toMatchObject({
      status: 404,
      type: 'NotFound',
    });
  });

  test('regenerateFDA NotFound error keeps explicit message text', async () => {
    const { regenerateFDA, collectionMock } = await loadMongoModule();

    collectionMock.findOneAndUpdate.mockResolvedValueOnce(null);
    collectionMock.findOne.mockResolvedValueOnce(null);

    await expect(regenerateFDA('svcA', 'fdaZ', '/sp')).rejects.toMatchObject({
      message: 'FDA fdaZ not found in service svcA and servicePath /sp',
    });
  });

  test('storeDA wraps update errors', async () => {
    const { storeDA, collectionMock } = await loadMongoModule();

    collectionMock.updateOne.mockRejectedValueOnce(new Error('store fail'));

    await expect(
      storeDA('svc', 'fdaA', '/sp', 'daA', 'desc', 'SELECT 1', []),
    ).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('retrieveFDAs wraps query errors', async () => {
    const { retrieveFDAs, collectionMock } = await loadMongoModule();

    collectionMock.find.mockImplementationOnce(() => {
      throw new Error('find fail');
    });

    await expect(retrieveFDAs('svc')).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('retrieveFDA wraps query errors', async () => {
    const { retrieveFDA, collectionMock } = await loadMongoModule();

    collectionMock.findOne.mockImplementationOnce(() => {
      throw new Error('findOne fail');
    });

    await expect(retrieveFDA('svc', 'fdaA', '/sp')).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('removeFDA wraps internal FDANotFound throw into MongoDBServerError', async () => {
    const { removeFDA, collectionMock } = await loadMongoModule();

    collectionMock.deleteOne.mockResolvedValueOnce({ deletedCount: 0 });

    await expect(removeFDA('svc', 'fdaA', '/sp')).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('retrieveDAs wraps aggregation errors', async () => {
    const { retrieveDAs, collectionMock } = await loadMongoModule();

    collectionMock.aggregate.mockImplementationOnce(() => {
      throw new Error('aggregate fail');
    });

    await expect(retrieveDAs('svc', 'fdaA', '/sp')).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('retrieveDA wraps query errors', async () => {
    const { retrieveDA, collectionMock } = await loadMongoModule();

    collectionMock.findOne.mockRejectedValueOnce(new Error('retrieve da fail'));

    await expect(retrieveDA('svc', 'fdaA', 'daA', '/sp')).rejects.toMatchObject(
      {
        status: 500,
        type: 'MongoDBServerError',
      },
    );
  });

  test('updateDA returns early when there are no fields to update', async () => {
    const { updateDA, collectionMock } = await loadMongoModule();

    await updateDA(
      'svc',
      'fdaA',
      '/sp',
      'daA',
      undefined,
      undefined,
      undefined,
    );

    expect(collectionMock.updateOne).not.toHaveBeenCalled();
  });

  test('updateDA wraps update errors', async () => {
    const { updateDA, collectionMock } = await loadMongoModule();

    collectionMock.updateOne.mockRejectedValueOnce(new Error('update fail'));

    await expect(
      updateDA('svc', 'fdaA', '/sp', 'daA', 'desc', 'SELECT 1', []),
    ).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });

  test('removeDA wraps update errors', async () => {
    const { removeDA, collectionMock } = await loadMongoModule();

    collectionMock.updateOne.mockRejectedValueOnce(new Error('remove fail'));

    await expect(removeDA('svc', 'fdaA', 'daA', '/sp')).rejects.toMatchObject({
      status: 500,
      type: 'MongoDBServerError',
    });
  });
});
