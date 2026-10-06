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
  jobs: jest.fn().mockResolvedValue([]),
  start: jest.fn().mockResolvedValue(undefined),
};

const getAgendaMock = jest.fn(() => agendaMock);
const processFDAAsyncMock = jest.fn().mockResolvedValue(undefined);
const processUploadFDAJobMock = jest.fn().mockResolvedValue(undefined);
const loggerMock = {
  debug: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
};
const childLoggerMock = {
  warn: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
};
const runWithLoggerMock = jest.fn(async (logger, fn) => fn());
const cleanPartitionMock = jest.fn();

async function loadFetcherModule() {
  jest.resetModules();

  agendaMock.define.mockClear();
  agendaMock.start.mockClear();
  agendaMock.jobs.mockReset();
  agendaMock.jobs.mockResolvedValue([]);
  getAgendaMock.mockClear();
  processFDAAsyncMock.mockClear();
  processUploadFDAJobMock.mockClear();
  cleanPartitionMock.mockClear();
  loggerMock.debug.mockClear();
  loggerMock.info.mockClear();
  loggerMock.error.mockClear();
  childLoggerMock.warn.mockClear();
  childLoggerMock.info.mockClear();
  childLoggerMock.error.mockClear();
  runWithLoggerMock.mockClear();

  await jest.unstable_mockModule('../../src/lib/jobs.js', () => ({
    getAgenda: getAgendaMock,
  }));

  await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
    processFDAAsync: processFDAAsyncMock,
    cleanPartition: cleanPartitionMock,
    processUploadFDAJob: processUploadFDAJobMock,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
    createChildLogger: jest.fn(() => childLoggerMock),
    runWithLogger: runWithLoggerMock,
  }));

  return import('../../src/fetcher.js');
}

describe('fetcher', () => {
  function getHandler(jobName) {
    const call = agendaMock.define.mock.calls.find(
      ([name]) => name === jobName,
    );
    if (!call) {
      throw new Error(`Handler for job "${jobName}" not found`);
    }

    return call[1];
  }

  test('startFetcher registers refresh job and starts agenda', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    expect(getAgendaMock).toHaveBeenCalledTimes(1);
    expect(agendaMock.define).toHaveBeenCalledWith(
      'refresh-fda',
      expect.any(Function),
      expect.objectContaining({
        concurrency: 1,
        lockLimit: 1,
      }),
    );
    expect(agendaMock.define).toHaveBeenCalledWith(
      'refresh-fda-recurring',
      expect.any(Function),
      expect.objectContaining({
        concurrency: 1,
        lockLimit: 1,
      }),
    );
    expect(agendaMock.define).toHaveBeenCalledWith(
      'consistency-refresh-fda-recurring',
      expect.any(Function),
      expect.objectContaining({
        concurrency: 1,
        lockLimit: 1,
      }),
    );
    expect(agendaMock.start).toHaveBeenCalledTimes(1);
    expect(loggerMock.info).toHaveBeenCalledWith('[Fetcher] Agenda started');
  });

  test('registered refresh handler delegates to processFDAAsync', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    const handler = getHandler('refresh-fda');

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
      undefined,
    );
  });

  describe('recurring refresh vs consistency refresh', () => {
    const buildJob = (name) => ({
      attrs: {
        name,
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

    test('recurring refresh is skipped while the consistency refresh is locked', async () => {
      const { startFetcher } = await loadFetcherModule();
      agendaMock.jobs.mockResolvedValue([{ attrs: {} }]);
      await startFetcher();

      await getHandler('refresh-fda-recurring')(
        buildJob('refresh-fda-recurring'),
      );

      expect(agendaMock.jobs).toHaveBeenCalledWith({
        name: 'consistency-refresh-fda-recurring',
        'data.service': 'svcA',
        'data.fdaId': 'fdaA',
        'data.servicePath': '/public',
        lockedAt: { $ne: null },
      });
      expect(processFDAAsyncMock).not.toHaveBeenCalled();
    });

    test('recurring refresh runs when no consistency refresh is locked', async () => {
      const { startFetcher } = await loadFetcherModule();
      await startFetcher();

      await getHandler('refresh-fda-recurring')(
        buildJob('refresh-fda-recurring'),
      );

      expect(processFDAAsyncMock).toHaveBeenCalledTimes(1);
    });

    test('recurring refresh runs if the consistency check fails', async () => {
      const { startFetcher } = await loadFetcherModule();
      agendaMock.jobs.mockRejectedValue(new Error('mongo down'));
      await startFetcher();

      await getHandler('refresh-fda-recurring')(
        buildJob('refresh-fda-recurring'),
      );

      expect(processFDAAsyncMock).toHaveBeenCalledTimes(1);
    });

    test('consistency refresh is never skipped', async () => {
      const { startFetcher } = await loadFetcherModule();
      agendaMock.jobs.mockResolvedValue([{ attrs: {} }]);
      await startFetcher();

      await getHandler('consistency-refresh-fda-recurring')(
        buildJob('consistency-refresh-fda-recurring'),
      );

      expect(agendaMock.jobs).not.toHaveBeenCalled();
      expect(processFDAAsyncMock).toHaveBeenCalledTimes(1);
    });
  });

  test('registered clean partition handler delegates to cleanPartition', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    const handler = getHandler('clean-partition');

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

    const handler = getHandler('clean-partition');

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

    expect(childLoggerMock.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.any(Error),
        fdaId: 'fdaA',
      }),
      'Job failed: clean-partition',
    );
  });

  test('registered upload handler delegates to processUploadFDAJob', async () => {
    const { startFetcher } = await loadFetcherModule();

    await startFetcher();

    const handler = getHandler('upload-fda');

    await handler({
      attrs: {
        data: {
          fdaId: 'fdaUploadA',
          service: 'svcA',
          servicePath: '/public',
          visibility: 'public',
          tempFilePath: '/tmp/file.csv',
          originalname: 'file.csv',
          mimetype: 'text/csv',
          description: 'upload test',
          timeColumn: 'event_date',
          objStgConf: { partition: 'day' },
          cached: true,
          defaultDataAccessEnabled: false,
        },
      },
    });

    expect(processUploadFDAJobMock).toHaveBeenCalledWith({
      fdaId: 'fdaUploadA',
      service: 'svcA',
      servicePath: '/public',
      visibility: 'public',
      tempFilePath: '/tmp/file.csv',
      originalname: 'file.csv',
      mimetype: 'text/csv',
      description: 'upload test',
      timeColumn: 'event_date',
      objStgConf: { partition: 'day' },
      cached: true,
      defaultDataAccessEnabled: false,
    });
  });

  test('registered upload handler catches errors', async () => {
    const { startFetcher } = await loadFetcherModule();

    processUploadFDAJobMock.mockRejectedValueOnce(new Error('upload failed'));

    await startFetcher();

    const handler = getHandler('upload-fda');

    await handler({
      attrs: {
        data: {
          fdaId: 'fdaUploadA',
          service: 'svcA',
          servicePath: '/public',
        },
      },
    });

    expect(childLoggerMock.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.any(Error),
        fdaId: 'fdaUploadA',
      }),
      'Job failed: upload-fda',
    );
  });
});
