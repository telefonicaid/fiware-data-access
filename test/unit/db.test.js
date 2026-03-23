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

const retrieveDAMock = jest.fn();
const retrieveFDAMock = jest.fn();
const duckCreateMock = jest.fn();
const loggerMock = {
  debug: jest.fn(),
};

function createDuckContext({ withDisconnect = true } = {}) {
  const configConn = {
    run: jest.fn().mockResolvedValue(undefined),
  };

  if (withDisconnect) {
    configConn.disconnect = jest.fn().mockResolvedValue(undefined);
  }

  const runtimeConn = {
    run: jest.fn().mockResolvedValue(undefined),
    prepare: jest.fn(),
    disconnect: jest.fn().mockResolvedValue(undefined),
  };

  const instance = {
    connect: jest
      .fn()
      .mockResolvedValueOnce(configConn)
      .mockResolvedValue(runtimeConn),
  };

  return { instance, configConn, runtimeConn };
}

async function loadDbModule({ retrieveDAResult, duckContext } = {}) {
  jest.resetModules();

  retrieveDAMock.mockReset().mockResolvedValue(
    retrieveDAResult ?? {
      query: 'SELECT id WHERE id = $id',
      params: [{ name: 'id', type: 'Number', required: false }],
    },
  );
  duckCreateMock.mockReset();
  loggerMock.debug.mockReset();

  const context = duckContext ?? createDuckContext();
  duckCreateMock.mockResolvedValue(context.instance);

  await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
    retrieveDA: retrieveDAMock,
    retrieveFDA: retrieveFDAMock,
  }));

  await jest.unstable_mockModule('@duckdb/node-api', () => ({
    DuckDBInstance: {
      create: duckCreateMock,
    },
  }));

  await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
    config: {
      objstg: {
        endpoint: 'minio:9000',
        usr: 'u',
        pass: 'p',
        maxPoolSize: 2,
      },
    },
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  const mod = await import('../../src/lib/utils/db.js');
  return { ...mod, ...context };
}

describe('db utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('releaseDBConnection resets and returns connection to pool', async () => {
    const { releaseDBConnection, getDBConnection } = await loadDbModule();

    const pooledConn = {
      run: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };

    await releaseDBConnection(pooledConn);

    expect(pooledConn.run).toHaveBeenCalledWith('RESET ALL;');

    const reused = await getDBConnection();
    expect(reused).toBe(pooledConn);
  });

  test('releaseDBConnection closes connection when reset fails', async () => {
    const { releaseDBConnection } = await loadDbModule();

    const badConn = {
      run: jest.fn().mockRejectedValue(new Error('reset failed')),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };

    await releaseDBConnection(badConn);

    expect(badConn.disconnect).toHaveBeenCalledTimes(1);
  });

  test('getDBConnection disconnects bootstrap config connection when available', async () => {
    const { getDBConnection, configConn } = await loadDbModule();

    await getDBConnection();

    expect(configConn.disconnect).toHaveBeenCalledTimes(1);
  });

  test('runPreparedStatement throws DaNotFound for missing DA', async () => {
    const { runPreparedStatement, runtimeConn } = await loadDbModule({
      retrieveDAResult: {},
    });

    await expect(
      runPreparedStatement(runtimeConn, 'svc', 'fdaA', 'missing', {}),
    ).rejects.toMatchObject({
      status: 404,
      type: 'DaNotFound',
    });

    expect(runtimeConn.prepare).not.toHaveBeenCalled();
  });

  test('runPreparedStatementStream returns close callback that closes statement', async () => {
    const { runPreparedStatementStream, runtimeConn } = await loadDbModule({
      retrieveDAResult: {
        query: 'SELECT id WHERE id = $id',
        params: [{ name: 'id', type: 'Number' }],
      },
    });
    retrieveFDAMock.mockReset().mockResolvedValue({});
    const stmt = {
      bind: jest.fn().mockResolvedValue(undefined),
      stream: jest.fn().mockResolvedValue('stream-ref'),
      close: jest.fn().mockResolvedValue(undefined),
    };
    runtimeConn.prepare.mockResolvedValueOnce(stmt);

    const result = await runPreparedStatementStream(
      runtimeConn,
      'svc',
      'fdaA',
      'daA',
      { id: 7 },
    );

    expect(result.stream).toBe('stream-ref');
    await result.close();

    expect(stmt.close).toHaveBeenCalledTimes(1);
  });

  test('runPreparedStatementStream closes stmt when bind fails and wraps error', async () => {
    const { runPreparedStatementStream, runtimeConn } = await loadDbModule({
      retrieveDAResult: {
        query: 'SELECT id WHERE id = $id',
        params: [{ name: 'id', type: 'Number' }],
      },
    });

    const stmt = {
      bind: jest.fn().mockRejectedValue(new Error('bind fail')),
      close: jest.fn().mockResolvedValue(undefined),
      stream: jest.fn(),
    };
    runtimeConn.prepare.mockResolvedValueOnce(stmt);

    await expect(
      runPreparedStatementStream(runtimeConn, 'svc', 'fdaA', 'daA', { id: 2 }),
    ).rejects.toMatchObject({
      status: 500,
      type: 'DuckDBServerError',
    });

    expect(stmt.close).toHaveBeenCalledTimes(1);
  });

  test('validateDAQuery wraps prepare failures', async () => {
    const { validateDAQuery, runtimeConn } = await loadDbModule();

    runtimeConn.prepare.mockRejectedValueOnce(new Error('syntax error'));

    await expect(
      validateDAQuery(runtimeConn, 'svc', 'fdaA', 'SELECT invalid'),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidDAQuery',
    });
  });

  test('validateDAQuery closes statement in finally block', async () => {
    const { validateDAQuery, runtimeConn } = await loadDbModule();

    const stmt = {
      close: jest.fn().mockResolvedValue(undefined),
    };
    runtimeConn.prepare.mockResolvedValueOnce(stmt);

    await validateDAQuery(runtimeConn, 'svc', 'fdaA', 'SELECT id');

    expect(stmt.close).toHaveBeenCalledTimes(1);
  });

  test('buildDAQuery rejects invalid user query values', async () => {
    const { buildDAQuery } = await loadDbModule();

    expect(() => buildDAQuery('svc', 'fdaA', null)).toThrow('Invalid DA query');
  });

  test('resolveDAParams coerces boolean string values', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ enabled: 'true', disabled: '0' }, [
      { name: 'enabled', type: 'Boolean' },
      { name: 'disabled', type: 'Boolean' },
    ]);

    expect(resolved).toEqual({ enabled: 1, disabled: 0 });
  });

  test('resolveDAParams coerces boolean aliases 1 and false', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ on: '1', off: 'false' }, [
      { name: 'on', type: 'Boolean' },
      { name: 'off', type: 'Boolean' },
    ]);

    expect(resolved).toEqual({ on: 1, off: 0 });
  });

  test('resolveDAParams rejects DateTime values that are not strings', async () => {
    const { resolveDAParams } = await loadDbModule();

    expect(() =>
      resolveDAParams({ when: 123 }, [
        { name: 'when', type: 'DateTime', required: true },
      ]),
    ).toThrow('Param "when" not of valid type (DateTime).');
  });

  test('resolveDAParams rejects malformed URL-encoded DateTime values', async () => {
    const { resolveDAParams } = await loadDbModule();

    expect(() =>
      resolveDAParams({ when: '%E0%A4%A' }, [
        { name: 'when', type: 'DateTime', required: true },
      ]),
    ).toThrow('Param "when" not of valid type (DateTime).');
  });

  test('resolveDAParams keeps valid DateTime values as ISO strings', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ when: '2025-01-01T00:00:00Z' }, [
      { name: 'when', type: 'DateTime' },
    ]);

    expect(resolved.when).toBe('2025-01-01T00:00:00.000Z');
  });

  test('validateDAQuery includes non-Error values in message text', async () => {
    const { validateDAQuery, runtimeConn } = await loadDbModule();

    runtimeConn.prepare.mockRejectedValueOnce('parse failed');

    await expect(
      validateDAQuery(runtimeConn, 'svc', 'fdaA', 'SELECT invalid'),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidDAQuery',
      message: 'DA query is not compatible with FDA fdaA: parse failed',
    });
  });

  test('resolveDAParams throws for unknown parameter types', async () => {
    const { resolveDAParams } = await loadDbModule();

    expect(() =>
      resolveDAParams({ custom: 'x' }, [
        { name: 'custom', type: 'UnknownType', required: true },
      ]),
    ).toThrow('Invalid type value in params.');
  });
});
