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
import { pipeline } from 'node:stream/promises';
import { Readable, Writable } from 'node:stream';

const mongoMocks = {
  retrieveQuotas: jest.fn(),
  aggregateFDAUsage: jest.fn(),
};

await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
  retrieveQuotas: mongoMocks.retrieveQuotas,
  aggregateFDAUsage: mongoMocks.aggregateFDAUsage,
}));

await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
  config: {
    quotas: {
      maxFDAsPerService: 10,
      maxFDAsPerServicePath: 0,
      maxBytesPerService: 1000,
      maxBytesPerServicePath: 0,
      maxBytesPerFDA: 0,
      maxFetchBytes: 0,
    },
  },
}));

const {
  resolveLimits,
  getUsage,
  assertCanCreateFDA,
  assertFDAWithinStorageLimit,
  createFetchByteGuard,
} = await import('../../src/lib/quotas.js');

describe('quotas', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mongoMocks.retrieveQuotas.mockResolvedValue([]);
    mongoMocks.aggregateFDAUsage.mockResolvedValue([]);
  });

  test('resolveLimits falls back to environment defaults and treats 0 as unlimited', async () => {
    await expect(resolveLimits('svc', '/a')).resolves.toEqual({
      service: { maxFDAs: 10, maxBytes: 1000 },
      servicePath: { maxFDAs: null, maxBytes: null },
      fda: { maxBytes: null, maxFetchBytes: null },
    });
  });

  test('resolveLimits prefers servicePath quota over service quota over environment', async () => {
    mongoMocks.retrieveQuotas.mockResolvedValue([
      {
        service: 'svc',
        servicePath: null,
        maxFDAs: 5,
        maxBytesPerServicePath: 300,
        maxBytesPerFDA: 100,
      },
      { service: 'svc', servicePath: '/a', maxFDAs: 2, maxBytesPerFDA: 50 },
    ]);

    await expect(resolveLimits('svc', '/a')).resolves.toEqual({
      service: { maxFDAs: 5, maxBytes: 1000 },
      servicePath: { maxFDAs: 2, maxBytes: 300 },
      fda: { maxBytes: 50, maxFetchBytes: null },
    });

    await expect(resolveLimits('svc', '/b')).resolves.toMatchObject({
      servicePath: { maxFDAs: null, maxBytes: 300 },
      fda: { maxBytes: 100 },
    });
  });

  test('a quota document can lift an environment limit by setting it to 0', async () => {
    mongoMocks.retrieveQuotas.mockResolvedValue([
      { service: 'svc', servicePath: null, maxFDAs: 0 },
    ]);

    const limits = await resolveLimits('svc', '/a');

    expect(limits.service.maxFDAs).toBeNull();
  });

  test('getUsage reports used and available room for service and servicePath', async () => {
    mongoMocks.aggregateFDAUsage.mockResolvedValue([
      { servicePath: '/a', fdas: 2, bytes: 300 },
      { servicePath: '/b', fdas: 1, bytes: 100 },
    ]);

    await expect(getUsage('svc', '/a')).resolves.toEqual({
      service: {
        limits: { maxFDAs: 10, maxBytes: 1000 },
        used: { fdas: 3, bytes: 400 },
        available: { fdas: 7, bytes: 600 },
      },
      servicePath: {
        limits: { maxFDAs: null, maxBytes: null },
        used: { fdas: 2, bytes: 300 },
        available: { fdas: null, bytes: null },
      },
      fda: { limits: { maxBytes: null, maxFetchBytes: null } },
    });
  });

  test('getUsage lists every servicePath when none is requested', async () => {
    const groups = [{ servicePath: '/a', fdas: 2, bytes: 300 }];
    mongoMocks.aggregateFDAUsage.mockResolvedValue(groups);

    const usage = await getUsage('svc');

    expect(usage.servicePath).toBeUndefined();
    expect(usage.byServicePath).toEqual(groups);
  });

  test('assertCanCreateFDA rejects when the service reached its FDA count', async () => {
    mongoMocks.aggregateFDAUsage.mockResolvedValue([
      { servicePath: '/a', fdas: 10, bytes: 0 },
    ]);

    await expect(assertCanCreateFDA('svc', '/b')).rejects.toMatchObject({
      status: 403,
      type: 'QuotaExceeded',
    });
  });

  test('assertCanCreateFDA rejects when the service storage is full', async () => {
    mongoMocks.aggregateFDAUsage.mockResolvedValue([
      { servicePath: '/a', fdas: 1, bytes: 1000 },
    ]);

    await expect(assertCanCreateFDA('svc', '/a')).rejects.toMatchObject({
      status: 403,
      type: 'QuotaExceeded',
    });
  });

  test('assertCanCreateFDA rejects when the servicePath reached its own limit', async () => {
    mongoMocks.retrieveQuotas.mockResolvedValue([
      { service: 'svc', servicePath: '/a', maxFDAs: 1 },
    ]);
    mongoMocks.aggregateFDAUsage.mockResolvedValue([
      { servicePath: '/a', fdas: 1, bytes: 0 },
    ]);

    await expect(assertCanCreateFDA('svc', '/a')).rejects.toMatchObject({
      type: 'QuotaExceeded',
    });
    await expect(assertCanCreateFDA('svc', '/b')).resolves.toBeUndefined();
  });

  test('assertFDAWithinStorageLimit only rejects above a defined limit', () => {
    expect(() => assertFDAWithinStorageLimit('fda1', 500, null)).not.toThrow();
    expect(() => assertFDAWithinStorageLimit('fda1', 500, 500)).not.toThrow();
    expect(() => assertFDAWithinStorageLimit('fda1', 501, 500)).toThrow(
      expect.objectContaining({ type: 'QuotaExceeded' }),
    );
  });

  test('createFetchByteGuard aborts the stream once the limit is exceeded', async () => {
    const sink = new Writable({
      write(_chunk, _encoding, callback) {
        callback();
      },
    });

    await expect(
      pipeline(
        Readable.from([Buffer.alloc(6), Buffer.alloc(6)]),
        createFetchByteGuard(10),
        sink,
      ),
    ).rejects.toMatchObject({ type: 'QuotaExceeded' });
  });

  test('createFetchByteGuard lets everything through when unlimited', async () => {
    const received = [];
    const sink = new Writable({
      write(chunk, _encoding, callback) {
        received.push(chunk.length);
        callback();
      },
    });

    await pipeline(
      Readable.from([Buffer.alloc(6), Buffer.alloc(6)]),
      createFetchByteGuard(null),
      sink,
    );

    expect(received).toEqual([6, 6]);
  });
});
