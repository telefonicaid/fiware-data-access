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
  retrieveFDAMock.mockReset().mockResolvedValue({});
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

  test('runPreparedStatement returns InvalidQueryParam when isTypeOf coercion fails', async () => {
    const { runPreparedStatement, runtimeConn } = await loadDbModule({
      retrieveDAResult: {
        query: 'SELECT id WHERE id = $id',
        params: [{ name: 'id', type: 'Number', required: true }],
      },
    });

    const stmt = {
      bind: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue({ getRowObjectsJson: () => [] }),
      close: jest.fn().mockResolvedValue(undefined),
    };
    runtimeConn.prepare.mockResolvedValueOnce(stmt);

    await expect(
      runPreparedStatement(runtimeConn, 'svc', 'fdaA', 'daA', {
        id: 'not-a-number',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidQueryParam',
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

  test('buildDAQuery rejects queries starting with FROM clause', async () => {
    const { buildDAQuery } = await loadDbModule();

    expect(() =>
      buildDAQuery('svc', 'fdaA', 'FROM read_parquet(...) SELECT *'),
    ).toThrow('DA query must not include FROM clause at start');

    expect(() => buildDAQuery('svc', 'fdaA', '  from   SELECT *')).toThrow(
      'DA query must not include FROM clause at start',
    );
  });

  test('buildDAQuery builds query without partition', async () => {
    const { buildDAQuery } = await loadDbModule();

    const result = buildDAQuery(
      'my-service',
      'fdaA',
      'SELECT * WHERE id = $1',
      false,
    );

    expect(result).toBe(
      "FROM read_parquet('s3://my-service/fdaA.parquet') SELECT * WHERE id = $1",
    );
  });

  test('buildDAQuery builds query with partition using wildcard path', async () => {
    const { buildDAQuery } = await loadDbModule();

    const result = buildDAQuery(
      'my-service',
      'fdaA',
      'SELECT * WHERE id = $1',
      true,
    );

    expect(result).toBe(
      "FROM read_parquet('s3://my-service/fdaA.parquet/**/*.parquet') SELECT * WHERE id = $1",
    );
  });

  test('resolveDAParams coerces boolean string values', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ enabled: 'true', disabled: '0' }, [
      { name: 'enabled', type: 'Boolean' },
      { name: 'disabled', type: 'Boolean' },
    ]);

    expect(resolved).toEqual({ enabled: true, disabled: false });
  });

  test('resolveDAParams coerces boolean aliases 1 and false', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ on: '1', off: 'false' }, [
      { name: 'on', type: 'Boolean' },
      { name: 'off', type: 'Boolean' },
    ]);

    expect(resolved).toEqual({ on: true, off: false });
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

  test('resolveDAParams keeps valid DateTime values as Date objects', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({ when: '2025-01-01T00:00:00Z' }, [
      { name: 'when', type: 'DateTime' },
    ]);

    expect(resolved.when).toBeInstanceOf(Date);
    expect(resolved.when.toISOString()).toBe('2025-01-01T00:00:00.000Z');
  });

  test('resolveDAParams applies default Boolean and DateTime preserving typed values', async () => {
    const { resolveDAParams } = await loadDbModule();

    const resolved = resolveDAParams({}, [
      { name: 'authorized', type: 'Boolean', default: true },
      {
        name: 'timeinstant',
        type: 'DateTime',
        default: '2020-08-17T18:25:28.332Z',
      },
    ]);

    expect(resolved.authorized).toBe(true);
    expect(resolved.timeinstant).toBeInstanceOf(Date);
    expect(resolved.timeinstant.toISOString()).toBe('2020-08-17T18:25:28.332Z');
  });

  test('runPreparedStatement normalizes DateTime and Boolean defaults for DuckDB binding', async () => {
    const { runPreparedStatement, runtimeConn } = await loadDbModule({
      retrieveDAResult: {
        query:
          'SELECT id WHERE authorized = $authorized AND timeinstant = $timeinstant',
        params: [
          { name: 'authorized', type: 'Boolean', default: true },
          {
            name: 'timeinstant',
            type: 'DateTime',
            default: '2020-08-17T18:25:28.332Z',
          },
        ],
      },
    });

    const stmt = {
      bind: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue({ getRowObjectsJson: () => [] }),
      close: jest.fn().mockResolvedValue(undefined),
    };
    runtimeConn.prepare.mockResolvedValueOnce(stmt);

    await runPreparedStatement(runtimeConn, 'svc', 'fdaA', 'daA', {});

    expect(stmt.bind).toHaveBeenCalledWith({
      authorized: 1,
      timeinstant: '2020-08-17T18:25:28.332Z',
    });
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

  test('checkParams does nothing when params is null or undefined', async () => {
    const { checkParams } = await loadDbModule();

    expect(checkParams(null)).toBeNull();
    expect(checkParams(undefined)).toBeUndefined();
  });

  test('checkParams throws FDAError when param missing type', async () => {
    const { checkParams } = await loadDbModule();

    expect(() => checkParams([{ name: 'id' }])).toThrow(
      'Type is a mandatory key in every param.',
    );
  });

  test('checkParams throws FDAError for invalid type value', async () => {
    const { checkParams } = await loadDbModule();

    expect(() => checkParams([{ name: 'id', type: 'InvalidType' }])).toThrow(
      'Invalid value in type key.',
    );
  });

  test('checkParams validates range: both values must be numbers', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([{ name: 'id', type: 'Number', range: ['1', 10] }]),
    ).toThrow('Both values of range param should be of type number');
  });

  test('checkParams validates range: first number must be smaller than second', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([{ name: 'id', type: 'Number', range: [10, 5] }]),
    ).toThrow('Fisrt number should be smaller than second number.');
  });

  test('checkParams validates range: cannot have more than 2 values', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([{ name: 'id', type: 'Number', range: [1, 5, 10] }]),
    ).toThrow('Range cant have more than two values.');
  });

  test('checkParams validates enum: values must be strings or numbers', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([
        { name: 'status', type: 'Text', enum: ['active', true, 'inactive'] },
      ]),
    ).toThrow('Values of enum param should be strings or numbers');
  });

  test('checkParams accepts valid param with valid type', async () => {
    const { checkParams } = await loadDbModule();

    expect(checkParams([{ name: 'id', type: 'Number' }])).toEqual([
      { name: 'id', type: 'Number' },
    ]);
  });

  test('checkParams accepts valid param with range', async () => {
    const { checkParams } = await loadDbModule();

    expect(
      checkParams([{ name: 'limit', type: 'Number', range: [1, 100] }]),
    ).toEqual([{ name: 'limit', type: 'Number', range: [1, 100] }]);
  });

  test('checkParams accepts valid param with enum', async () => {
    const { checkParams } = await loadDbModule();

    expect(
      checkParams([
        { name: 'status', type: 'Text', enum: ['active', 'inactive', 1, 2] },
      ]),
    ).toEqual([
      { name: 'status', type: 'Text', enum: ['active', 'inactive', 1, 2] },
    ]);
  });

  test('checkParams accepts multiple valid params', async () => {
    const { checkParams } = await loadDbModule();

    expect(
      checkParams([
        { name: 'id', type: 'Number', required: true },
        { name: 'status', type: 'Text', enum: ['active', 'inactive'] },
        { name: 'limit', type: 'Number', range: [1, 100] },
      ]),
    ).toEqual([
      { name: 'id', type: 'Number', required: true },
      { name: 'status', type: 'Text', enum: ['active', 'inactive'] },
      { name: 'limit', type: 'Number', range: [1, 100] },
    ]);
  });

  test('checkParams coerces and canonicalizes defaults for storage', async () => {
    const { checkParams } = await loadDbModule();

    const result = checkParams([
      { name: 'enabled', type: 'Boolean', default: '1' },
      {
        name: 'when',
        type: 'DateTime',
        default: '2020-08-17T18:25:28.332Z',
      },
    ]);

    expect(result).toEqual([
      { name: 'enabled', type: 'Boolean', default: true },
      {
        name: 'when',
        type: 'DateTime',
        default: '2020-08-17T18:25:28.332Z',
      },
    ]);
  });

  test('checkParams accepts Date object default for DateTime and stores ISO string', async () => {
    const { checkParams } = await loadDbModule();

    const result = checkParams([
      {
        name: 'when',
        type: 'DateTime',
        default: new Date('2020-08-17T18:25:28.332Z'),
      },
    ]);

    expect(result[0].default).toBe('2020-08-17T18:25:28.332Z');
  });

  test('checkParams rejects incompatible default type', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([{ name: 'enabled', type: 'Boolean', default: 'notBool' }]),
    ).toThrow('Default value for param "enabled" not of valid type (Boolean).');
  });

  test('checkParams rejects default outside declared range', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([
        {
          name: 'minAge',
          type: 'Number',
          default: 10,
          range: [20, 50],
        },
      ]),
    ).toThrow(
      'Default value for param "minAge" not in valid param range [20,50].',
    );
  });

  test('checkParams rejects default not present in enum', async () => {
    const { checkParams } = await loadDbModule();

    expect(() =>
      checkParams([
        {
          name: 'name',
          type: 'Text',
          default: 'bob',
          enum: ['ana', 'carlos'],
        },
      ]),
    ).toThrow('Default value for param "name" not in param enum [ana,carlos].');
  });

  test('resolveDAParams uses type coercion and throws on invalid value (isTypeOf path)', async () => {
    const { resolveDAParams } = await loadDbModule();

    expect(
      resolveDAParams({ id: '42' }, [{ name: 'id', type: 'Number' }]),
    ).toEqual({ id: 42 });

    expect(() =>
      resolveDAParams({ id: 'notanumber' }, [{ name: 'id', type: 'Number' }]),
    ).toThrow('Param "id" not of valid type (Number).');
  });

  test('toParquet builds copy SQL with no partition', async () => {
    const { toParquet } = await loadDbModule();
    const conn = { run: jest.fn().mockResolvedValue(undefined) };

    await toParquet(
      conn,
      'test-bucket/source',
      'test-bucket/target',
      'ts',
      'none',
      false,
    );

    expect(conn.run).toHaveBeenCalledTimes(1);
    const sql = conn.run.mock.calls[0][0];
    expect(sql).toContain("FROM read_csv_auto('s3://test-bucket/source')");
    expect(sql).toContain("TO 's3://test-bucket/target'");
    expect(sql).toContain('FORMAT PARQUET');
    expect(sql).not.toContain('PARTITION_BY');
  });

  test('toParquet calls getPartitionConf path for day partition and includes partition by clause', async () => {
    const { toParquet } = await loadDbModule();
    const conn = { run: jest.fn().mockResolvedValue(undefined) };

    await toParquet(
      conn,
      'test-bucket/source',
      'test-bucket/target',
      'timestamp',
      'day',
      true,
    );

    const sql = conn.run.mock.calls[0][0];
    expect(sql).toContain('year(timestamp::TIMESTAMP) as year');
    expect(sql).toContain('PARTITION_BY (year, month, day)');
    expect(sql).toContain('COMPRESSION ZSTD');
  });

  test('toParquet throws PartitionError when partitionType requires timeColumn but missing', async () => {
    const { toParquet } = await loadDbModule();
    const conn = { run: jest.fn().mockResolvedValue(undefined) };

    expect(() => toParquet(conn, 'a', 'b', undefined, 'day', false)).toThrow(
      'Missing timeColumn value.',
    );
  });
});
