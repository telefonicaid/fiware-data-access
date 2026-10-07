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

import { beforeEach, describe, expect, jest, test } from '@jest/globals';

const recordFDAAccessesMock = jest.fn();
const onDAQueryMock = jest.fn();
const loggerMock = { warn: jest.fn() };

await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
  recordFDAAccesses: recordFDAAccessesMock,
}));

await jest.unstable_mockModule('../../src/lib/metrics.js', () => ({
  onDAQuery: onDAQueryMock,
}));

await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
  getBasicLogger: () => loggerMock,
}));

await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
  config: { accessTracking: { flushIntervalMs: 60000 } },
}));

const { recordAccess, flushAccesses, stopAccessTracker } = await import(
  '../../src/lib/accessTracker.js'
);

describe('accessTracker', () => {
  beforeEach(async () => {
    recordFDAAccessesMock.mockReset().mockResolvedValue(undefined);
    await stopAccessTracker();
    jest.clearAllMocks();
  });

  test('aggregates accesses per FDA and per DA before flushing', async () => {
    const scope = { service: 'svc', servicePath: '/a', fdaId: 'fda1' };
    recordAccess({ ...scope, daId: 'da1' });
    recordAccess({ ...scope, daId: 'da1' });
    recordAccess({ ...scope, daId: 'da2' });
    recordAccess({ service: 'svc', servicePath: '/a', fdaId: 'fda2' });

    await flushAccesses();

    expect(recordFDAAccessesMock).toHaveBeenCalledTimes(1);
    const [entries] = recordFDAAccessesMock.mock.calls[0];
    expect(entries).toEqual([
      {
        ...scope,
        count: 3,
        lastAccessAt: expect.any(Date),
        das: {
          da1: { count: 2, lastAccessAt: expect.any(Date) },
          da2: { count: 1, lastAccessAt: expect.any(Date) },
        },
      },
      {
        service: 'svc',
        servicePath: '/a',
        fdaId: 'fda2',
        count: 1,
        lastAccessAt: expect.any(Date),
        das: {},
      },
    ]);
    expect(onDAQueryMock).toHaveBeenCalledTimes(4);
  });

  test('does not write anything when there are no pending accesses', async () => {
    await flushAccesses();

    expect(recordFDAAccessesMock).not.toHaveBeenCalled();
  });

  test('clears pending accesses after a flush', async () => {
    recordAccess({ service: 'svc', servicePath: '/a', fdaId: 'fda1' });

    await flushAccesses();
    await flushAccesses();

    expect(recordFDAAccessesMock).toHaveBeenCalledTimes(1);
  });

  test('a failed flush is logged and never thrown to the caller', async () => {
    recordFDAAccessesMock.mockRejectedValueOnce(new Error('mongo down'));
    recordAccess({ service: 'svc', servicePath: '/a', fdaId: 'fda1' });

    await expect(flushAccesses()).resolves.toBeUndefined();

    expect(loggerMock.warn).toHaveBeenCalledTimes(1);
  });
});
