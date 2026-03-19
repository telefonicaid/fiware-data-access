// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access

import { describe, expect, jest, test } from '@jest/globals';

const agendaInstance = {
  on: jest.fn(),
  drain: jest.fn().mockResolvedValue(undefined),
};

const loggerMock = {
  error: jest.fn(),
  info: jest.fn(),
};

const agendaCtorMock = jest.fn(() => agendaInstance);
const mongoBackendCtorMock = jest.fn(() => ({ mocked: true }));

async function loadJobsModule() {
  jest.resetModules();

  agendaCtorMock.mockClear();
  mongoBackendCtorMock.mockClear();
  agendaInstance.on.mockClear();
  agendaInstance.drain.mockClear();
  loggerMock.error.mockClear();
  loggerMock.info.mockClear();

  await jest.unstable_mockModule('agenda', () => ({
    Agenda: agendaCtorMock,
  }));

  await jest.unstable_mockModule('@agendajs/mongo-backend', () => ({
    MongoBackend: mongoBackendCtorMock,
  }));

  await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
    config: {
      mongo: {
        uri: 'mongodb://mongo:27017/fda-tests',
      },
    },
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  return import('../../src/lib/jobs.js');
}

describe('jobs', () => {
  test('initAgenda is idempotent and wires backend with expected settings', async () => {
    const { initAgenda } = await loadJobsModule();

    const first = initAgenda();
    const second = initAgenda();

    expect(first).toBe(second);
    expect(mongoBackendCtorMock).toHaveBeenCalledWith({
      address: 'mongodb://mongo:27017/fda-tests',
      collection: 'agendaJobs',
    });
    expect(agendaCtorMock).toHaveBeenCalledWith({
      backend: { mocked: true },
      removeOnComplete: true,
    });
    expect(agendaInstance.on).toHaveBeenCalledWith(
      'error',
      expect.any(Function),
    );
  });

  test('getAgenda throws before initAgenda', async () => {
    const { getAgenda } = await loadJobsModule();

    expect(() => getAgenda()).toThrow('Agenda not initialized');
  });

  test('getAgenda returns initialized agenda', async () => {
    const { getAgenda, initAgenda } = await loadJobsModule();

    const agenda = initAgenda();

    expect(getAgenda()).toBe(agenda);
  });

  test('shutdownAgenda drains initialized agenda and logs completion', async () => {
    const { initAgenda, shutdownAgenda } = await loadJobsModule();

    initAgenda();
    await shutdownAgenda();

    expect(agendaInstance.drain).toHaveBeenCalledTimes(1);
    expect(loggerMock.info).toHaveBeenCalledWith('[Jobs] Agenda drained');
  });

  test('shutdownAgenda is a no-op when initAgenda has not been called', async () => {
    const { shutdownAgenda } = await loadJobsModule();

    await shutdownAgenda();

    expect(agendaInstance.drain).not.toHaveBeenCalled();
  });
});
