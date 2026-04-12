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

import { describe, expect, jest, test } from '@jest/globals';

const agendaMock = {
  define: jest.fn(),
  start: jest.fn().mockResolvedValue(undefined),
};

const getAgendaMock = jest.fn(() => agendaMock);
const processFDAAsyncMock = jest.fn().mockResolvedValue(undefined);
const loggerMock = {
  info: jest.fn(),
  error: jest.fn(),
};
const cleanPartitionMock = jest.fn();

async function loadFetcherModule() {
  jest.resetModules();

  agendaMock.define.mockClear();
  agendaMock.start.mockClear();
  getAgendaMock.mockClear();
  processFDAAsyncMock.mockClear();
  cleanPartitionMock.mockClear();
  loggerMock.info.mockClear();

  await jest.unstable_mockModule('../../src/lib/jobs.js', () => ({
    getAgenda: getAgendaMock,
  }));

  await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
    processFDAAsync: processFDAAsyncMock,
    cleanPartition: cleanPartitionMock,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  return import('../../src/fetcher.js');
}

describe('fetcher', () => {
  test('startFetcher registers refresh job and starts agenda', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    expect(getAgendaMock).toHaveBeenCalledTimes(1);
    expect(agendaMock.define).toHaveBeenCalledWith(
      'refresh-fda',
      expect.any(Function),
    );
    expect(agendaMock.start).toHaveBeenCalledTimes(1);
    expect(loggerMock.info).toHaveBeenCalledWith('[Fetcher] Agenda started');
  });

  test('registered refresh handler delegates to processFDAAsync', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    const handler = agendaMock.define.mock.calls[0][1];

    await handler({
      attrs: {
        data: {
          fdaId: 'fdaA',
          query: 'SELECT 1',
          service: 'svcA',
          servicePath: '/public',
          timeColumn: 'timeinstant',
          refreshPolicy: {},
          objStgConf: {},
        },
      },
    });

    expect(processFDAAsyncMock).toHaveBeenCalledWith(
      'fdaA',
      'SELECT 1',
      'svcA',
      '/public',
      'timeinstant',
      {},
      {},
      false,
    );
  });

  test('registered clean partition handler delegates to cleanPartition', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    const handler = agendaMock.define.mock.calls[1][1];

    await handler({
      attrs: {
        data: {
          fdaId: 'fdaA',
          service: 'svcA',
          servicePath: '/public',
          windowSize: 'day',
          objStgConf: {},
        },
      },
    });

    expect(cleanPartitionMock).toHaveBeenCalledWith(
      'svcA',
      'fdaA',
      'day',
      {},
      '/public',
    );
  });

  test('registered clean partition handler catches errors', async () => {
    const { startFetcher } = await loadFetcherModule();

    cleanPartitionMock.mockRejectedValueOnce(
      new Error('partition clean failed'),
    );

    await startFetcher();

    const handler = agendaMock.define.mock.calls[1][1];

    await handler({
      attrs: {
        data: {
          fdaId: 'fdaA',
          service: 'svcA',
          servicePath: '/public',
          windowSize: 'day',
          objStgConf: {},
        },
      },
    });

    expect(loggerMock.error).toHaveBeenCalledWith(
      'Fetcher error: ',
      expect.any(Error),
    );
  });
});
