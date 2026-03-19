// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access

import { describe, expect, jest, test } from '@jest/globals';

const agendaMock = {
  define: jest.fn(),
  start: jest.fn().mockResolvedValue(undefined),
};

const getAgendaMock = jest.fn(() => agendaMock);
const processFDAAsyncMock = jest.fn().mockResolvedValue(undefined);
const loggerMock = {
  info: jest.fn(),
};

async function loadFetcherModule() {
  jest.resetModules();

  agendaMock.define.mockClear();
  agendaMock.start.mockClear();
  getAgendaMock.mockClear();
  processFDAAsyncMock.mockClear();
  loggerMock.info.mockClear();

  await jest.unstable_mockModule('../../src/lib/jobs.js', () => ({
    getAgenda: getAgendaMock,
  }));

  await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
    processFDAAsync: processFDAAsyncMock,
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
        },
      },
    });

    expect(processFDAAsyncMock).toHaveBeenCalledWith(
      'fdaA',
      'SELECT 1',
      'svcA',
    );
  });
});
