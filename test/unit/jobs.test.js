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
