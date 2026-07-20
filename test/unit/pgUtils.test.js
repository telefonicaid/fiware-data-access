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

import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import { FDAError } from '../../src/lib/fdaError.js';

let currentClient;
let currentPool;

const clientCtorMock = jest.fn(function clientFactory() {
  return currentClient;
});

const poolCtorMock = jest.fn(function poolFactory() {
  return currentPool;
});

const copyToMock = jest.fn((query) => ({ copyQuery: query }));

const cursorCtorMock = jest.fn(function MockCursor(text, values) {
  this.text = text;
  this.values = values;
});

const newUploadMock = jest.fn();

const loggerMock = {
  debug: jest.fn(),
  info: jest.fn(),
};

await jest.unstable_mockModule('pg', () => ({
  default: {
    Client: clientCtorMock,
    Pool: poolCtorMock,
    types: {
      setTypeParser: jest.fn(),
      builtins: {
        BOOL: 16,
        INT2: 21,
        INT4: 23,
        INT8: 20,
        FLOAT4: 700,
        FLOAT8: 701,
        NUMERIC: 1700,
        DATE: 1082,
        TIME: 1083,
        TIMETZ: 1266,
        TIMESTAMP: 1114,
        TIMESTAMPTZ: 1184,
        JSON: 114,
        JSONB: 3802,
        UUID: 2950,
        BYTEA: 17,
      },
    },
  },
}));

await jest.unstable_mockModule('pg-copy-streams', () => ({
  to: copyToMock,
}));

await jest.unstable_mockModule('pg-cursor', () => ({
  default: cursorCtorMock,
}));

await jest.unstable_mockModule('../../src/lib/utils/aws.js', () => ({
  newUpload: newUploadMock,
}));

await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
  getBasicLogger: () => loggerMock,
}));

await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
  config: {
    pg: {
      pool: {
        max: 10,
        idleTimeoutMillis: 10000,
        connectionTimeoutMillis: 5000,
        databaseIdleTimeoutMillis: 50,
      },
    },
  },
}));

const {
  getPgClient,
  uploadTable,
  runPgQuery,
  createPgCursorReader,
  validatePostgresQuery,
  getDuckDBTypeFromPostgresField,
} = await import('../../src/lib/utils/pg.js');
const { closePgPools } = await import('../../src/lib/utils/pg.js');
const { config } = await import('../../src/lib/fdaConfig.js');

describe('pg utils', () => {
  beforeEach(async () => {
    jest.clearAllMocks();
    jest.useRealTimers();

    await closePgPools();

    currentClient = {
      query: jest.fn(),
      release: jest.fn(),
    };

    currentPool = {
      connect: jest.fn().mockResolvedValue(currentClient),
      end: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
    };
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('getPgClient builds a pg client with expected config', () => {
    const client = getPgClient('u1', 'p1', 'h1', 15432, 'db1');

    expect(clientCtorMock).toHaveBeenCalledWith({
      user: 'u1',
      password: 'p1',
      host: 'h1',
      port: 15432,
      database: 'db1',
    });
    expect(client).toBe(currentClient);
  });

  test('runPgQuery returns rows on success', async () => {
    currentClient.query.mockResolvedValue({ rows: [{ id: 1 }] });

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    const rows = await runPgQuery(creds, 'SELECT 1', []);

    expect(currentPool.connect).toHaveBeenCalledTimes(1);
    expect(currentClient.query).toHaveBeenCalledWith('SELECT 1', []);
    expect(rows).toEqual([{ id: 1 }]);
    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('runPgQuery wraps non-FDA errors', async () => {
    currentClient.query.mockRejectedValue(new Error('query exploded'));

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    await expect(runPgQuery(creds, 'SELECT bad()', [])).rejects.toMatchObject({
      status: 500,
      type: 'PostgresServerError',
      message: 'Error running fresh query: query exploded',
    });

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('runPgQuery rethrows FDAError instances', async () => {
    const fdaError = new FDAError(429, 'TooManyFreshQueries', 'limit');
    currentClient.query.mockRejectedValue(fdaError);

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    await expect(runPgQuery(creds, 'SELECT 1', [])).rejects.toBe(fdaError);

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('validatePostgresQuery validates a query and allows a declared timeColumn', async () => {
    currentClient.query.mockResolvedValue({
      fields: [{ name: 'id' }, { name: 'timeinstant' }],
    });

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };

    await expect(
      validatePostgresQuery(creds, 'SELECT id, timeinstant FROM users', {
        timeColumn: 'timeinstant',
      }),
    ).resolves.toBeNull();

    expect(currentClient.query).toHaveBeenCalledWith(
      'SELECT * FROM (SELECT id, timeinstant FROM users) AS fda_validation LIMIT 0',
    );
    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('validatePostgresQuery returns typed schema metadata when requested', async () => {
    currentClient.query.mockResolvedValue({
      fields: [
        { name: 'id', dataTypeID: 23 },
        { name: 'timeinstant', dataTypeID: 1184 },
        { name: 'payload', dataTypeID: 999999 },
      ],
    });

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };

    await expect(
      validatePostgresQuery(
        creds,
        'SELECT id, timeinstant, payload FROM users',
        {
          returnColumns: true,
        },
      ),
    ).resolves.toEqual({
      columns: ['id', 'timeinstant', 'payload'],
      fields: [
        { name: 'id', postgresTypeId: 23, duckdbType: 'INTEGER' },
        {
          name: 'timeinstant',
          postgresTypeId: 1184,
          duckdbType: 'TIMESTAMPTZ',
        },
        { name: 'payload', postgresTypeId: 999999, duckdbType: 'VARCHAR' },
      ],
    });

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('getDuckDBTypeFromPostgresField maps PostgreSQL field types to DuckDB types', () => {
    expect(getDuckDBTypeFromPostgresField({ dataTypeID: 16 })).toBe('BOOLEAN');
    expect(getDuckDBTypeFromPostgresField({ dataTypeID: 20 })).toBe('BIGINT');
    expect(getDuckDBTypeFromPostgresField({ dataTypeID: 1700 })).toBe('DOUBLE');
    expect(getDuckDBTypeFromPostgresField({ dataTypeID: 1184 })).toBe(
      'TIMESTAMPTZ',
    );
    expect(getDuckDBTypeFromPostgresField({ dataTypeID: 999999 })).toBe(
      'VARCHAR',
    );
  });

  // test/unit/pgUtils.test.js

  test('validatePostgresQuery rejects when the declared timeColumn is not in the schema', async () => {
    currentClient.query.mockResolvedValue({
      fields: [{ name: 'id' }, { name: 'name' }],
    });

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };

    await expect(
      validatePostgresQuery(creds, 'SELECT id, name FROM users', {
        timeColumn: 'timeinstant',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: expect.stringContaining(
        'Time column "timeinstant" is not present in the SELECT clause',
      ),
    });

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('validatePostgresQuery wraps SQL errors as FDAError', async () => {
    currentClient.query.mockRejectedValue(new Error('syntax exploded'));

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };

    await expect(
      validatePostgresQuery(creds, 'SELECT bad(', { timeColumn: 'ts' }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Invalid Postgres FDA query: syntax exploded',
    });

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('createPgCursorReader reads chunk and closes idempotently', async () => {
    const cursorObject = {
      read: jest.fn((size, callback) => callback(null, [{ id: size }])),
      close: jest.fn((callback) => callback(null)),
    };
    currentClient.query.mockReturnValue(cursorObject);

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    const reader = await createPgCursorReader(creds, 'SELECT id', [9], 100);

    await expect(reader.readNextChunk()).resolves.toEqual([{ id: 100 }]);

    await reader.close();
    await reader.close();

    expect(cursorCtorMock).toHaveBeenCalledWith('SELECT id', [9]);
    expect(cursorObject.close).toHaveBeenCalledTimes(1);
    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('createPgCursorReader wraps non-FDA errors', async () => {
    currentPool.connect.mockRejectedValue(new Error('connect exploded'));

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    await expect(
      createPgCursorReader(creds, 'SELECT 1', [], 10),
    ).rejects.toMatchObject({
      status: 500,
      type: 'PostgresServerError',
      message: 'Error streaming fresh query: connect exploded',
    });
  });

  test('uploadTable converts upload failures into FDAError and closes resources', async () => {
    const stream = {
      destroy: jest.fn(),
    };
    currentClient.query.mockReturnValue(stream);

    const uploader = {
      on: jest.fn(),
      done: jest.fn().mockRejectedValue(new Error('upload exploded')),
    };
    newUploadMock.mockReturnValue(uploader);

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    await expect(
      uploadTable({}, 'bucketA', creds, 'SELECT * FROM t', 'path/file'),
    ).rejects.toMatchObject({
      status: 503,
      type: 'UploadError',
      message: 'Error uploading FDA to object storage: upload exploded',
    });

    expect(copyToMock).toHaveBeenCalledWith(
      'COPY (SELECT * FROM t) TO STDOUT WITH CSV HEADER',
    );
    expect(stream.destroy).toHaveBeenCalledWith(expect.any(Error));
    expect(stream.destroy).toHaveBeenCalledTimes(2);
    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('uploadTable uploads successfully', async () => {
    const stream = {
      destroy: jest.fn(),
    };
    currentClient.query.mockReturnValue(stream);

    const uploader = {
      on: jest.fn(),
      done: jest.fn().mockResolvedValue(undefined),
    };
    newUploadMock.mockReturnValue(uploader);

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db',
    };
    await uploadTable({}, 'bucketA', creds, 'SELECT * FROM t', 'path/file');

    expect(loggerMock.debug).toHaveBeenCalledWith(
      'Upload completed successfully',
    );
    expect(stream.destroy).toHaveBeenCalledTimes(1);
    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('closePgPools closes all created pools', async () => {
    currentClient.query.mockResolvedValue({ rows: [] });

    const creds1 = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_1',
    };
    const creds2 = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_2',
    };
    await runPgQuery(creds1, 'SELECT 1', []);
    await runPgQuery(creds2, 'SELECT 1', []);
    await closePgPools();

    expect(poolCtorMock).toHaveBeenCalledTimes(2);
    expect(currentPool.end).toHaveBeenCalled();
  });

  test('closes inactive per-database pool after timeout', async () => {
    jest.useFakeTimers();
    currentClient.query.mockResolvedValue({ rows: [{ id: 1 }] });
    currentPool.totalCount = 1;
    currentPool.idleCount = 1;
    currentPool.waitingCount = 0;

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_idle',
    };
    await runPgQuery(creds, 'SELECT 1', []);

    await jest.advanceTimersByTimeAsync(60);

    expect(currentPool.end).toHaveBeenCalledTimes(1);
  });

  test('does not schedule idle close when database idle timeout is disabled', async () => {
    jest.useFakeTimers();
    currentClient.query.mockResolvedValue({ rows: [{ id: 1 }] });
    currentPool.totalCount = 1;
    currentPool.idleCount = 1;
    currentPool.waitingCount = 0;

    const previousTimeout = config.pg.pool.databaseIdleTimeoutMillis;
    config.pg.pool.databaseIdleTimeoutMillis = 0;

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_no_idle_close',
    };
    await runPgQuery(creds, 'SELECT 1', []);
    await jest.advanceTimersByTimeAsync(120);

    expect(currentPool.end).not.toHaveBeenCalled();

    config.pg.pool.databaseIdleTimeoutMillis = previousTimeout;
  });

  test('returns early when pool entry does not exist at release time', async () => {
    currentClient.query.mockImplementation(async () => {
      await closePgPools();
      return { rows: [{ id: 1 }] };
    });

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_missing_entry',
    };
    await expect(runPgQuery(creds, 'SELECT 1', [])).resolves.toEqual([
      { id: 1 },
    ]);

    expect(currentClient.release).toHaveBeenCalledTimes(1);
  });

  test('reschedules idle close while pool still has active work', async () => {
    jest.useFakeTimers();
    currentClient.query.mockResolvedValue({ rows: [{ id: 1 }] });
    currentPool.totalCount = 2;
    currentPool.idleCount = 1;
    currentPool.waitingCount = 0;

    const creds = {
      user: 'u',
      password: 'p',
      host: 'h',
      port: 5432,
      database: 'svc_db_busy_pool',
    };
    await runPgQuery(creds, 'SELECT 1', []);

    await jest.advanceTimersByTimeAsync(60);
    expect(currentPool.end).not.toHaveBeenCalled();

    currentPool.totalCount = 1;
    currentPool.idleCount = 1;
    currentPool.waitingCount = 0;

    await jest.advanceTimersByTimeAsync(60);
    expect(currentPool.end).toHaveBeenCalledTimes(1);
  });
});
