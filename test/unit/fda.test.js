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
import { FDAError } from '../../src/lib/fdaError.js';

const dbMocks = {
  runPreparedStatement: jest.fn(),
  runPreparedStatementStream: jest.fn(),
  getDBConnection: jest.fn(),
  releaseDBConnection: jest.fn(),
  toParquet: jest.fn(),
  checkParams: jest.fn(),
  resolveDAParams: jest.fn(),
  validateDAQuery: jest.fn(),
  extractDate: jest.fn(),
  PARTITION_TYPES: ['day', 'week', 'month', 'year', 'none'],
  refreshIntervalPartitionCheck: jest.fn(),
};

const pgMocks = {
  uploadTable: jest.fn(),
  runPgQuery: jest.fn(),
  createPgCursorReader: jest.fn(),
};

const awsMocks = {
  getS3Client: jest.fn(),
  dropFile: jest.fn(),
  dropFiles: jest.fn(),
  moveObject: jest.fn(),
  listObjects: jest.fn(),
};

const mongoMocks = {
  createFDAMongo: jest.fn(),
  regenerateFDA: jest.fn(),
  retrieveFDAs: jest.fn(),
  retrieveFDA: jest.fn(),
  storeDA: jest.fn(),
  removeFDA: jest.fn(),
  retrieveDAs: jest.fn(),
  retrieveDA: jest.fn(),
  updateDA: jest.fn(),
  removeDA: jest.fn(),
  updateFDAStatus: jest.fn(),
};

const jobsMocks = {
  getAgenda: jest.fn(),
};

await jest.unstable_mockModule('../../src/lib/jobs.js', () => ({
  getAgenda: jobsMocks.getAgenda,
}));

await jest.unstable_mockModule('../../src/lib/utils/db.js', () => ({
  runPreparedStatement: dbMocks.runPreparedStatement,
  runPreparedStatementStream: dbMocks.runPreparedStatementStream,
  getDBConnection: dbMocks.getDBConnection,
  releaseDBConnection: dbMocks.releaseDBConnection,
  toParquet: dbMocks.toParquet,
  checkParams: dbMocks.checkParams,
  resolveDAParams: dbMocks.resolveDAParams,
  validateDAQuery: dbMocks.validateDAQuery,
  extractDate: dbMocks.extractDate,
  PARTITION_TYPES: dbMocks.PARTITION_TYPES,
  refreshIntervalPartitionCheck: dbMocks.refreshIntervalPartitionCheck,
}));

await jest.unstable_mockModule('../../src/lib/utils/pg.js', () => ({
  uploadTable: pgMocks.uploadTable,
  runPgQuery: pgMocks.runPgQuery,
  createPgCursorReader: pgMocks.createPgCursorReader,
}));

await jest.unstable_mockModule('../../src/lib/utils/aws.js', () => ({
  getS3Client: awsMocks.getS3Client,
  dropFile: awsMocks.dropFile,
  dropFiles: awsMocks.dropFiles,
  moveObject: awsMocks.moveObject,
  listObjects: awsMocks.listObjects,
}));

await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
  createFDAMongo: mongoMocks.createFDAMongo,
  regenerateFDA: mongoMocks.regenerateFDA,
  retrieveFDAs: mongoMocks.retrieveFDAs,
  retrieveFDA: mongoMocks.retrieveFDA,
  storeDA: mongoMocks.storeDA,
  removeFDA: mongoMocks.removeFDA,
  retrieveDAs: mongoMocks.retrieveDAs,
  retrieveDA: mongoMocks.retrieveDA,
  updateDA: mongoMocks.updateDA,
  removeDA: mongoMocks.removeDA,
  updateFDAStatus: mongoMocks.updateFDAStatus,
}));

await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
  config: {
    roles: {
      syncQueries: true,
    },
    freshQueries: {
      maxConcurrent: 2,
    },
    objstg: {
      protocol: 'http',
      endpoint: 'minio:9000',
      usr: 'user',
      pass: 'pass',
    },
  },
}));

const {
  executeQuery,
  executeFDAQuery,
  executeFDAQueryStream,
  executeQueryStream,
  createDA,
  fetchFDA,
  getFDA,
  updateFDA,
  processFDAAsync,
  deleteFDA,
  getDA,
  putDA,
  getFDAs,
  cleanPartition,
} = await import('../../src/lib/fda.js');

function createReqRes() {
  const reqHandlers = {};

  const req = {
    on: jest.fn((event, callback) => {
      reqHandlers[event] = callback;
    }),
  };

  const res = {
    setHeader: jest.fn(),
    write: jest.fn().mockReturnValue(true),
    once: jest.fn((event, callback) => {
      if (event === 'drain') {
        callback();
      }
    }),
    end: jest.fn(),
  };

  return { req, res, reqHandlers };
}

describe('fda fresh query execution', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT id WHERE id = $id',
      params: { id: { type: 'number' } },
    });
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 1 AS id;',
      visibility: 'private',
      servicePath: '/servicepath',
    });

    dbMocks.resolveDAParams.mockImplementation((params) => params);
    pgMocks.runPgQuery.mockResolvedValue([{ id: 1 }]);
  });

  test('builds and executes fresh query with positional parameters', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT id WHERE id = $id OR id > $id ORDER BY id',
      params: { id: { type: 'number' } },
    });
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 7::bigint AS id;',
      visibility: 'private',
      servicePath: '/servicepath',
    });
    dbMocks.resolveDAParams.mockReturnValue({ id: 7 });
    pgMocks.runPgQuery.mockResolvedValue([{ id: 7n }]);

    const rows = await executeQuery({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA', id: 7 },
      fresh: true,
    });

    expect(pgMocks.runPgQuery).toHaveBeenCalledWith(
      'svc',
      'SELECT id FROM (SELECT 7::bigint AS id) AS fda_source WHERE id = $1 OR id > $1 ORDER BY id',
      [7],
    );
    expect(rows).toEqual([{ id: 7 }]);
  });

  test('serializes Date values as ISO strings in fresh JSON results', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT id',
      params: {},
    });

    pgMocks.runPgQuery.mockResolvedValue([
      { date: new Date('2026-04-08T10:11:12.000Z'), count: 1n },
    ]);

    const rows = await executeQuery({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      fresh: true,
    });

    expect(rows).toEqual([{ date: '2026-04-08T10:11:12.000Z', count: 1 }]);
  });

  test('executes FDA direct fresh query against PostgreSQL source', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 7::bigint AS id;',
      visibility: 'private',
      servicePath: '/servicepath',
      cached: false,
    });
    pgMocks.runPgQuery.mockResolvedValue([{ id: 7n }]);

    const rows = await executeFDAQuery({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      fdaId: 'fdaA',
    });

    expect(pgMocks.runPgQuery).toHaveBeenCalledWith(
      'svc',
      'SELECT 7::bigint AS id',
      [],
    );
    expect(rows).toEqual([{ id: 7 }]);
  });

  test('rejects direct FDA query when FDA is cached (not only-fresh)', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 1',
      visibility: 'public',
      servicePath: '/servicepath',
      cached: true,
    });

    await expect(
      executeFDAQuery({
        service: 'svc',
        visibility: 'public',
        servicePath: '/servicepath',
        fdaId: 'fdaA',
      }),
    ).rejects.toMatchObject({ status: 409, type: 'FDANotOnlyFresh' });
  });

  test('streams FDA direct fresh query rows and handles backpressure', async () => {
    const { req, res } = createReqRes();
    const cursorReader = {
      readNextChunk: jest
        .fn()
        .mockResolvedValueOnce([{ total: 12n }])
        .mockResolvedValueOnce([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 12::bigint AS total;',
      visibility: 'private',
      servicePath: '/servicepath',
      cached: false,
    });
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    res.write.mockReturnValueOnce(false).mockReturnValueOnce(true);

    await executeFDAQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      fdaId: 'fdaA',
      req,
      res,
      format: 'ndjson',
    });

    expect(pgMocks.createPgCursorReader).toHaveBeenCalledWith(
      'svc',
      'SELECT 12::bigint AS total',
      [],
      250,
    );
    expect(res.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'application/x-ndjson',
    );
  });

  test('releases fresh slot when createFreshFDARowSource fails to create cursor', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 1',
      visibility: 'private',
      servicePath: '/servicepath',
      cached: false,
    });
    pgMocks.createPgCursorReader.mockRejectedValueOnce(
      new Error('pg connection refused'),
    );

    const { req, res } = createReqRes();

    await expect(
      executeFDAQueryStream({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        fdaId: 'fdaA',
        req,
        res,
        format: 'ndjson',
      }),
    ).rejects.toThrow('pg connection refused');
  });

  test('throws DaNotFound when DA does not exist', async () => {
    mongoMocks.retrieveDA.mockResolvedValue(undefined);

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'missing' },
        fresh: true,
      }),
    ).rejects.toMatchObject({ status: 404, type: 'DaNotFound' });
  });

  test('throws FDANotFound when FDA does not exist', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue(undefined);
    mongoMocks.retrieveFDAs.mockResolvedValue([]);

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'missing', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toMatchObject({ status: 404, type: 'FDANotFound' });
  });

  test('rejects DA queries that start with FROM', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'FROM users',
      params: {},
    });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toThrow('DA query must not include FROM clause at start');
  });

  test('rejects DA queries not starting with SELECT', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'UPDATE users SET x = 1',
      params: {},
    });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toThrow(
      'Fresh query mode requires DA query to start with SELECT',
    );
  });

  test('rejects DA queries with empty projection', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT WHERE id = 1',
      params: {},
    });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toThrow('DA query must contain a SELECT projection');
  });

  test('rejects DA projections that include FROM', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT id FROM users WHERE id = 1',
      params: {},
    });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toThrow(
      'DA query must not include FROM clause. It is managed internally',
    );
  });

  test('throws InvalidQueryParam when a named param is missing', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      query: 'SELECT id WHERE id = $id AND city = $city',
      params: {},
    });
    dbMocks.resolveDAParams.mockReturnValue({ id: 1 });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
        fresh: true,
      }),
    ).rejects.toMatchObject({ status: 400, type: 'InvalidQueryParam' });
  });

  test('rethrows FDAError instances from Postgres layer', async () => {
    const pgError = new FDAError(500, 'PostgresServerError', 'db failed');
    pgMocks.runPgQuery.mockRejectedValue(pgError);

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
        fresh: true,
      }),
    ).rejects.toBe(pgError);
  });

  test('rejects query execution when requested visibility does not match FDA visibility', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      query: 'SELECT 7::bigint AS id;',
      visibility: 'public',
      servicePath: '/servicepath',
    });

    await expect(
      executeQuery({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        fresh: true,
      }),
    ).rejects.toMatchObject({ status: 403, type: 'VisibilityMismatch' });
  });

  test('streams NDJSON rows and handles backpressure', async () => {
    const { req, res } = createReqRes();
    const cursorReader = {
      readNextChunk: jest
        .fn()
        .mockResolvedValueOnce([{ total: 12n }])
        .mockResolvedValueOnce([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    res.write.mockReturnValueOnce(false).mockReturnValueOnce(true);

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
      req,
      res,
      fresh: true,
    });

    expect(res.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'application/x-ndjson',
    );
    expect(res.write).toHaveBeenCalledWith('{"total":12}\n');
    expect(res.once).toHaveBeenCalledWith('drain', expect.any(Function));
    expect(cursorReader.close).toHaveBeenCalledTimes(1);
    expect(res.end).toHaveBeenCalledTimes(1);
  });

  test('serializes Date values as ISO strings in fresh NDJSON stream', async () => {
    const { req, res } = createReqRes();
    const cursorReader = {
      readNextChunk: jest
        .fn()
        .mockResolvedValueOnce([
          { date: new Date('2026-04-08T10:11:12.000Z'), count: 1n },
        ])
        .mockResolvedValueOnce([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
      req,
      res,
      fresh: true,
    });

    expect(res.write).toHaveBeenCalledWith(
      '{"date":"2026-04-08T10:11:12.000Z","count":1}\n',
    );
    expect(res.end).toHaveBeenCalledTimes(1);
  });

  test('streams CSV rows in fresh mode', async () => {
    const { req, res } = createReqRes();
    const cursorReader = {
      readNextChunk: jest
        .fn()
        .mockResolvedValueOnce([
          { date: new Date('2026-04-08T10:11:12.000Z'), count: 1n },
        ])
        .mockResolvedValueOnce([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
      req,
      res,
      fresh: true,
      format: 'csv',
    });

    expect(res.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'text/csv; charset=utf-8',
    );
    expect(res.write).toHaveBeenNthCalledWith(1, 'date,count\n');
    expect(res.write).toHaveBeenNthCalledWith(
      2,
      '2026-04-08T10:11:12.000Z,1\n',
    );
    expect(res.end).toHaveBeenCalledTimes(1);
  });

  test('serializes null and undefined as empty fields in CSV stream', async () => {
    const { req, res } = createReqRes();
    const conn = {};
    const stream = {
      columnNames: jest.fn().mockReturnValue(['a', 'b', 'c']),
      fetchChunk: jest
        .fn()
        .mockResolvedValueOnce({
          rowCount: 1,
          getRows: () => [[null, undefined, 1n]],
        })
        .mockResolvedValueOnce({ rowCount: 0, getRows: () => [] }),
    };
    const close = jest.fn().mockResolvedValue(undefined);

    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatementStream.mockResolvedValue({ stream, close });
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
      format: 'csv',
    });

    expect(res.write).toHaveBeenNthCalledWith(1, 'a,b,c\n');
    expect(res.write).toHaveBeenNthCalledWith(2, ',,1\n');
    expect(close).toHaveBeenCalledTimes(1);
    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith(conn);
  });

  test('escapes CSV values and waits for drain when write returns false', async () => {
    const { req, res } = createReqRes();
    const conn = {};
    const stream = {
      columnNames: jest.fn().mockReturnValue(['txt']),
      fetchChunk: jest
        .fn()
        .mockResolvedValueOnce({
          rowCount: 1,
          getRows: () => [['a,b"c']],
        })
        .mockResolvedValueOnce({ rowCount: 0, getRows: () => [] }),
    };
    const close = jest.fn().mockResolvedValue(undefined);

    res.write.mockReturnValueOnce(false).mockReturnValue(true);
    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatementStream.mockResolvedValue({ stream, close });
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
      format: 'csv',
    });

    expect(res.once).toHaveBeenCalledWith('drain', expect.any(Function));
    expect(res.write).toHaveBeenNthCalledWith(2, '"a,b""c"\n');
    expect(close).toHaveBeenCalledTimes(1);
  });

  test('releases DB connection if stream initialization fails in CSV mode', async () => {
    const { req, res } = createReqRes();
    const conn = {};

    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatementStream.mockRejectedValue(
      new Error('stream init failed'),
    );
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    await expect(
      executeQueryStream({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        req,
        res,
        fresh: false,
        format: 'csv',
      }),
    ).rejects.toThrow('stream init failed');

    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith(conn);
  });

  test('closes cursor when request close event is triggered', async () => {
    const { req, res, reqHandlers } = createReqRes();
    const cursorReader = {
      readNextChunk: jest.fn().mockResolvedValue([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
      req,
      res,
      fresh: true,
    });

    reqHandlers.close();
    await Promise.resolve();

    expect(cursorReader.close).toHaveBeenCalled();
  });

  test('rethrows FDAError from cursor creation', async () => {
    const { req, res } = createReqRes();
    const streamError = new FDAError(
      500,
      'PostgresServerError',
      'stream failed',
    );
    pgMocks.createPgCursorReader.mockRejectedValue(streamError);

    await expect(
      executeQueryStream({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
        req,
        res,
        fresh: true,
      }),
    ).rejects.toBe(streamError);
  });

  test('serializes DuckDB timestamp micros in non-fresh NDJSON stream', async () => {
    const { req, res } = createReqRes();
    const conn = {};
    const stream = {
      columnNames: jest.fn().mockReturnValue(['date', 'count']),
      fetchChunk: jest
        .fn()
        .mockResolvedValueOnce({
          rowCount: 1,
          getRows: () => [[{ micros: 1700524800000000 }, 1n]],
        })
        .mockResolvedValueOnce({ rowCount: 0, getRows: () => [] }),
    };
    const close = jest.fn().mockResolvedValue(undefined);

    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatementStream.mockResolvedValue({ stream, close });
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
    });

    expect(res.write).toHaveBeenCalledWith(
      '{"date":"2023-11-21T00:00:00.000Z","count":1}\n',
    );
    expect(close).toHaveBeenCalledTimes(1);
    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith(conn);
  });

  test('serializes DuckDB timestamp micros in non-fresh CSV stream', async () => {
    const { req, res } = createReqRes();
    const conn = {};
    const stream = {
      columnNames: jest.fn().mockReturnValue(['date']),
      fetchChunk: jest
        .fn()
        .mockResolvedValueOnce({
          rowCount: 1,
          getRows: () => [[{ micros: 1700524800000000 }]],
        })
        .mockResolvedValueOnce({ rowCount: 0, getRows: () => [] }),
    };
    const close = jest.fn().mockResolvedValue(undefined);

    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatementStream.mockResolvedValue({ stream, close });
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    await executeQueryStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
      format: 'csv',
    });

    expect(res.write).toHaveBeenNthCalledWith(1, 'date\n');
    expect(res.write).toHaveBeenNthCalledWith(2, '2023-11-21T00:00:00.000Z\n');
  });

  test('serializes DuckDB timestamp micros in non-fresh JSON results', async () => {
    const conn = {};

    dbMocks.getDBConnection.mockResolvedValue(conn);
    dbMocks.runPreparedStatement.mockResolvedValue([
      { date: { micros: 1700524800000000 }, count: 1n },
    ]);
    mongoMocks.retrieveFDA.mockResolvedValue({
      status: 'completed',
      visibility: 'private',
      servicePath: '/servicepath',
      lastFetch: new Date('2026-04-08T00:00:00.000Z').toISOString(),
    });

    const rows = await executeQuery({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      fresh: false,
    });

    expect(rows).toEqual([{ date: '2023-11-21T00:00:00.000Z', count: 1 }]);
    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith(conn);
  });
});

describe('fetchFDA', () => {
  const agenda = {
    now: jest.fn(),
    every: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.dropFile.mockResolvedValue(undefined);
    mongoMocks.createFDAMongo.mockResolvedValue(undefined);
    mongoMocks.removeFDA.mockResolvedValue(undefined);
    dbMocks.getDBConnection.mockResolvedValue({});
    dbMocks.releaseDBConnection.mockResolvedValue(undefined);
    dbMocks.toParquet.mockResolvedValue(undefined);
    pgMocks.uploadTable.mockResolvedValue(undefined);
    jobsMocks.getAgenda.mockReturnValue(agenda);
    agenda.now.mockResolvedValue(undefined);
    agenda.every.mockResolvedValue(undefined);
    dbMocks.refreshIntervalPartitionCheck.mockReturnValue(true);
  });

  test('creates one-row parquet synchronously and schedules immediate fetch', async () => {
    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
    );

    expect(mongoMocks.createFDAMongo).toHaveBeenCalledWith(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      { type: 'none' },
      'timeinstant',
      undefined,
      true,
    );
    expect(pgMocks.uploadTable).toHaveBeenCalledWith(
      {},
      'svc',
      'svc',
      'SELECT * FROM (SELECT id FROM users) AS fda_one_row LIMIT 1',
      'servicepath/fda1',
    );
    expect(dbMocks.toParquet).toHaveBeenCalledWith(
      {},
      'svc/servicepath/fda1.csv',
      'svc/servicepath/fda1.parquet',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith(
      {},
      'svc',
      'servicepath/fda1.csv',
    );
    expect(agenda.now).toHaveBeenCalledWith('refresh-fda', {
      fdaId: 'fda1',
      query: 'SELECT id FROM users;',
      service: 'svc',
      servicePath: '/servicepath',
      timeColumn: 'timeinstant',
      refreshPolicy: {
        type: 'none',
      },
      objStgConf: undefined,
    });
    expect(agenda.every).not.toHaveBeenCalled();
  });

  test('creates default DA with optional filters, time range and pagination params', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [
        { column_name: 'entity-id' },
        { column_name: 'limit' },
        { column_name: 'timeinstant' },
      ],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
      undefined,
      true,
    );

    expect(describeRun).toHaveBeenCalledWith(
      "DESCRIBE SELECT * FROM read_parquet('s3://svc/servicepath/fda1.parquet')",
    );
    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total WHERE ($entity_id IS NULL OR "entity-id" = $entity_id) AND ($limit IS NULL OR "limit" = $limit) AND ($timeinstant IS NULL OR DATE_TRUNC(\'millisecond\', CAST("timeinstant" AS TIMESTAMP)) = DATE_TRUNC(\'millisecond\', CAST($timeinstant AS TIMESTAMP))) AND ($start IS NULL OR CAST("timeinstant" AS TIMESTAMP) >= CAST($start AS TIMESTAMP)) AND ($finish IS NULL OR CAST("timeinstant" AS TIMESTAMP) <= CAST($finish AS TIMESTAMP)) LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'entity_id', default: null },
        { name: 'limit', default: null },
        { name: 'timeinstant', default: null },
        { name: 'start', default: null },
        { name: 'finish', default: null },
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('creates default DA without time filters when FDA has no timeColumn', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [{ column_name: 'name' }],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      undefined,
      undefined,
      true,
    );

    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total WHERE ($name IS NULL OR "name" = $name) LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'name', default: null },
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('creates default DA with time filters when timeColumn matches columns case-insensitively', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [
        { column_name: 'TimeInstant' },
        { column_name: 'name' },
      ],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
      undefined,
      true,
    );

    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total WHERE ($timeinstant IS NULL OR DATE_TRUNC(\'millisecond\', CAST("TimeInstant" AS TIMESTAMP)) = DATE_TRUNC(\'millisecond\', CAST($timeinstant AS TIMESTAMP))) AND ($name IS NULL OR "name" = $name) AND ($start IS NULL OR CAST("TimeInstant" AS TIMESTAMP) >= CAST($start AS TIMESTAMP)) AND ($finish IS NULL OR CAST("TimeInstant" AS TIMESTAMP) <= CAST($finish AS TIMESTAMP)) LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'timeinstant', default: null },
        { name: 'name', default: null },
        { name: 'start', default: null },
        { name: 'finish', default: null },
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('creates default DA without time range when configured timeColumn is not present in parquet columns', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [
        { column_name: 'timeinstant' },
        { column_name: 'name' },
      ],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'observed_at',
      undefined,
      true,
    );

    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total WHERE ($timeinstant IS NULL OR "timeinstant" = $timeinstant) AND ($name IS NULL OR "name" = $name) LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'timeinstant', default: null },
        { name: 'name', default: null },
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('creates default DA with pagination only when parquet has no columns', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
      undefined,
      true,
    );

    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('sanitizes generated default DA param names and avoids reserved collisions', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [
        { column_name: '123value' },
        { column_name: '!!!' },
        { column_name: 'offset' },
        { column_name: 'my"col' },
      ],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      undefined,
      undefined,
      true,
    );

    expect(mongoMocks.storeDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
      'defaultDataAccess',
      'Default Data Access providing access to whole FDA data. It has parameters for all columns in the FDA.',
      'SELECT *, COUNT(*) OVER() as __total WHERE ($col_123value IS NULL OR "123value" = $col_123value) AND ($col IS NULL OR "!!!" = $col) AND ($offset IS NULL OR "offset" = $offset) AND ($my_col IS NULL OR "my""col" = $my_col) LIMIT CAST($pageSize AS BIGINT) OFFSET CAST($pageStart AS BIGINT)',
      [
        { name: 'col_123value', default: null },
        { name: 'col', default: null },
        { name: 'offset', default: null },
        { name: 'my_col', default: null },
        { name: 'pageSize', default: '9223372036854775807' },
        { name: 'pageStart', default: 0 },
      ],
    );
  });

  test('does not create default DA when disabled', async () => {
    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
      undefined,
      false,
    );

    expect(mongoMocks.storeDA).not.toHaveBeenCalled();
  });

  test('does not create parquet snippet or default DA when FDA is only-fresh', async () => {
    await fetchFDA(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      'public',
      '/servicepath',
      'test FDA',
      {
        type: 'none',
      },
      'timeinstant',
      undefined,
      true,
      false,
    );

    expect(pgMocks.uploadTable).not.toHaveBeenCalled();
    expect(dbMocks.toParquet).not.toHaveBeenCalled();
    expect(mongoMocks.storeDA).not.toHaveBeenCalled();
    expect(agenda.now).not.toHaveBeenCalled();
  });

  test('rejects DA creation for only-fresh FDAs', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
      cached: false,
    });

    await expect(
      createDA(
        'svc',
        'fda1',
        'da1',
        'desc',
        'SELECT id',
        [],
        'public',
        '/servicepath',
      ),
    ).rejects.toMatchObject({
      status: 409,
      type: 'FDAOnlyFresh',
    });

    expect(dbMocks.validateDAQuery).not.toHaveBeenCalled();
    expect(mongoMocks.storeDA).not.toHaveBeenCalled();
  });

  test('rolls back FDA provisioning when default DA creation fails', async () => {
    const describeRun = jest.fn().mockResolvedValue({
      getRowObjectsJson: () => [{ column_name: 'name' }],
    });

    dbMocks.getDBConnection
      .mockReset()
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ run: describeRun })
      .mockResolvedValueOnce({});
    dbMocks.releaseDBConnection.mockReset().mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    dbMocks.validateDAQuery.mockRejectedValueOnce(
      new Error('invalid default DA'),
    );
    mongoMocks.retrieveFDA.mockResolvedValue({
      visibility: 'public',
      servicePath: '/servicepath',
    });
    mongoMocks.retrieveDA.mockResolvedValue(null);

    await expect(
      fetchFDA(
        'fda1',
        'SELECT id FROM users;',
        'svc',
        'public',
        '/servicepath',
        'test FDA',
        {
          type: 'none',
        },
        'timeinstant',
        undefined,
        true,
      ),
    ).rejects.toMatchObject({
      status: 500,
      type: 'DefaultDataAccessCreationError',
    });

    expect(mongoMocks.removeFDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith(
      {},
      'svc',
      'servicepath/fda1.csv',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith(
      {},
      'svc',
      'servicepath/fda1.parquet',
    );
    expect(agenda.now).not.toHaveBeenCalled();
  });

  test('schedules periodic refresh for interval policy', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'interval',
        params: { refreshInterval: '10 minutes' },
      },
      'timeinstant',
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '10 minutes',
      'refresh-fda',
      {
        fdaId: 'fda1',
        query: 'SELECT timeinstant, 1',
        service: 'svc',
        servicePath: '/servicepath',
        timeColumn: 'timeinstant',
        refreshPolicy: {
          type: 'interval',
          params: { refreshInterval: '10 minutes' },
        },
        objStgConf: undefined,
      },
      {
        skipImmediate: true,
        unique: {
          name: 'refresh-fda',
          'data.service': 'svc',
          'data.fdaId': 'fda1',
          'data.servicePath': '/servicepath',
        },
      },
    );
  });

  test('fails when refresh interval is larger than partition size', async () => {
    dbMocks.refreshIntervalPartitionCheck.mockReturnValue(false);

    await expect(
      fetchFDA(
        'fda1',
        'SELECT 1',
        'svc',
        'public',
        '/servicePath',
        'desc',
        {
          type: 'interval',
          params: { refreshInterval: '2 days' },
        },
        'timeinstant',
        { partition: 'day' },
      ),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message:
        'Refresh interval "2 days" must be smaller or equal than partition size "day".',
    });

    expect(agenda.every).not.toHaveBeenCalled();
  });
  test('rolls back FDA provisioning and rethrows when parquet creation fails', async () => {
    const uploadError = new Error('S3 unreachable');
    pgMocks.uploadTable.mockRejectedValue(uploadError);

    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', '/servicepath', 'desc', {
        type: 'none',
      }),
    ).rejects.toBe(uploadError);

    expect(mongoMocks.removeFDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      '/servicepath',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith(
      {},
      'svc',
      'servicepath/fda1.csv',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith(
      {},
      'svc',
      'servicepath/fda1.parquet',
    );
    expect(agenda.now).not.toHaveBeenCalled();
  });

  test('fetchFDA with interval policy and windowSize schedules clean-partition job', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'interval',
        params: {
          refreshInterval: '1 hour',
          fetchSize: 'week',
          windowSize: 'week',
        },
      },
      'timeinstant',
      { partition: 'week' },
      false,
      true,
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '1 hour',
      'clean-partition',
      {
        fdaId: 'fda1',
        service: 'svc',
        servicePath: '/servicepath',
        windowSize: 'week',
        objStgConf: { partition: 'week' },
      },
      {
        skipImmediate: true,
        unique: {
          name: 'clean-partition',
          'data.service': 'svc',
          'data.fdaId': 'fda1',
          'data.servicePath': '/servicepath',
        },
      },
    );
  });

  test('fetchFDA with window refresh policy schedules cleanup', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'window',
        params: {
          refreshInterval: '0 0 * * *',
          fetchSize: 'day',
          windowSize: 'day',
        },
      },
      'timeinstant',
      {
        partition: 'day',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      expect.any(String),
      'refresh-fda',
      expect.any(Object),
      expect.any(Object),
    );
  });

  test('fetchFDA throws when servicePath is missing', async () => {
    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', undefined, 'desc', {
        type: 'none',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidServicePath',
    });
  });

  test('fetchFDA throws when servicePath is missing', async () => {
    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', undefined, 'desc', {
        type: 'none',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidServicePath',
    });
  });
});

describe('updateFDA', () => {
  const agenda = {
    now: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jobsMocks.getAgenda.mockReturnValue(agenda);
    agenda.now.mockResolvedValue(undefined);
    mongoMocks.retrieveFDA.mockResolvedValue({
      fdaId: 'fda42',
      cached: true,
      servicePath: '/servicepath',
      visibility: 'public',
    });
    mongoMocks.regenerateFDA.mockResolvedValue({
      query: 'SELECT id FROM users',
    });
  });

  test('regenerates FDA and schedules refresh job immediately', async () => {
    await updateFDA('svc', 'fda42', undefined, '/servicepath');

    expect(mongoMocks.regenerateFDA).toHaveBeenCalledWith(
      'svc',
      'fda42',
      '/servicepath',
    );
    expect(agenda.now).toHaveBeenCalledWith('refresh-fda', {
      fdaId: 'fda42',
      query: 'SELECT id FROM users',
      service: 'svc',
      servicePath: '/servicepath',
      timeColumn: undefined,
      objStgConf: undefined,
      partitionFlag: true,
    });
  });

  test('regenerates sliding-window FDA and schedules refresh and clean job immediately', async () => {
    mongoMocks.regenerateFDA.mockResolvedValue({
      query: 'SELECT id FROM users',
      refreshPolicy: {
        type: 'window',
        params: {
          refreshInterval: '0 0 * * *',
          fetchSize: 'day',
          windowSize: 'day',
        },
      },
    });
    await updateFDA('svc', 'fda42', undefined, '/servicepath');

    expect(mongoMocks.regenerateFDA).toHaveBeenCalledWith(
      'svc',
      'fda42',
      '/servicepath',
    );
    expect(agenda.now).toHaveBeenCalledWith('refresh-fda', {
      fdaId: 'fda42',
      query: 'SELECT id FROM users',
      service: 'svc',
      servicePath: '/servicepath',
      timeColumn: undefined,
      refreshPolicy: {
        type: 'window',
        params: {
          refreshInterval: '0 0 * * *',
          fetchSize: 'day',
          windowSize: 'day',
        },
      },
      objStgConf: undefined,
      partitionFlag: true,
    });

    expect(agenda.now).toHaveBeenCalledWith('clean-partition', {
      fdaId: 'fda42',
      service: 'svc',
      windowSize: 'day',
      objStgConf: undefined,
    });
  });

  test('checks accessibility with visibility before scheduling update', async () => {
    await updateFDA('svc', 'fda42', 'public', '/servicepath');

    expect(mongoMocks.retrieveFDA).toHaveBeenCalledWith(
      'svc',
      'fda42',
      '/servicepath',
    );
    expect(mongoMocks.regenerateFDA).toHaveBeenCalledWith(
      'svc',
      'fda42',
      '/servicepath',
    );
    expect(agenda.now).toHaveBeenCalledWith(
      'refresh-fda',
      expect.objectContaining({
        fdaId: 'fda42',
        service: 'svc',
      }),
    );
  });

  test('throws when trying to manually refresh a fresh-only FDA', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      fdaId: 'fda42',
      cached: false,
      servicePath: '/servicepath',
      visibility: 'public',
    });

    await expect(
      updateFDA('svc', 'fda42', undefined, '/servicepath'),
    ).rejects.toMatchObject({
      status: 409,
      type: 'FDAOnlyFresh',
    });

    expect(mongoMocks.regenerateFDA).not.toHaveBeenCalled();
    expect(agenda.now).not.toHaveBeenCalled();
  });
});

//
describe('processFDAAsync', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.dropFile.mockResolvedValue(undefined);
    dbMocks.getDBConnection.mockResolvedValue({});
    dbMocks.releaseDBConnection.mockResolvedValue(undefined);
    dbMocks.toParquet.mockResolvedValue(undefined);
    pgMocks.uploadTable.mockResolvedValue(undefined);
    mongoMocks.updateFDAStatus.mockResolvedValue(undefined);
  });

  test('updates status through successful async FDA processing lifecycle', async () => {
    await processFDAAsync('fda1', 'SELECT 1', 'svc', '/servicepath');

    expect(mongoMocks.updateFDAStatus).toHaveBeenNthCalledWith(
      1,
      'svc',
      'fda1',
      '/servicepath',
      'fetching',
      10,
    );
    expect(mongoMocks.updateFDAStatus).toHaveBeenNthCalledWith(
      2,
      'svc',
      'fda1',
      '/servicepath',
      'fetching',
      20,
    );
    expect(mongoMocks.updateFDAStatus).toHaveBeenNthCalledWith(
      3,
      'svc',
      'fda1',
      '/servicepath',
      'transforming',
      60,
    );
    expect(mongoMocks.updateFDAStatus).toHaveBeenNthCalledWith(
      4,
      'svc',
      'fda1',
      '/servicepath',
      'uploading',
      80,
    );
    expect(mongoMocks.updateFDAStatus).toHaveBeenNthCalledWith(
      5,
      'svc',
      'fda1',
      '/servicepath',
      'completed',
      100,
    );
  });

  test('uses normalized bucket name while preserving original database name', async () => {
    await processFDAAsync('fda1', 'SELECT 1', 'service_name', '/servicepath');

    expect(pgMocks.uploadTable).toHaveBeenCalledWith(
      {},
      'service-name',
      'service_name',
      'SELECT 1',
      'servicepath/fda1',
    );
  });

  test('builds hourly sliding-window query when fetchSize is hour', async () => {
    await processFDAAsync(
      'fda1',
      'SELECT id, observed_at FROM public.events',
      'svc',
      '/servicepath',
      'observed_at',
      {
        type: 'window',
        params: {
          refreshInterval: '1 hour',
          fetchSize: 'hour',
        },
      },
    );

    expect(pgMocks.uploadTable).toHaveBeenCalledWith(
      {},
      'svc',
      'svc',
      expect.stringContaining(
        "SELECT * FROM (SELECT id, observed_at FROM public.events) q WHERE observed_at >= TIMESTAMP '",
      ),
      'servicepath/fda1',
    );
    expect(pgMocks.uploadTable.mock.calls[0][3]).toContain(
      'AND observed_at < NOW()',
    );
  });

  test('marks FDA as failed and rethrows when upload fails', async () => {
    pgMocks.uploadTable.mockRejectedValue(new Error('upload failed'));

    await expect(
      processFDAAsync('fda2', 'SELECT 2', 'svc', '/servicepath'),
    ).rejects.toThrow('upload failed');

    expect(mongoMocks.updateFDAStatus).toHaveBeenCalledWith(
      'svc',
      'fda2',
      '/servicepath',
      'failed',
      0,
      'upload failed',
    );
  });
});

describe('deleteFDA', () => {
  const agenda = {
    cancel: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jobsMocks.getAgenda.mockReturnValue(agenda);
    agenda.cancel.mockResolvedValue(undefined);
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.dropFile.mockResolvedValue(undefined);
    mongoMocks.removeFDA.mockResolvedValue(undefined);
  });

  test('drops parquet, removes FDA and cancels agenda job', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo-id',
      visibility: 'private',
      servicePath: '/servicepath',
    });
    awsMocks.listObjects.mockResolvedValue(['routeTo/fdaA.parquet']);

    await deleteFDA('svc', 'fdaA', 'private', '/servicepath');

    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'svc', [
      'routeTo/fdaA.parquet',
    ]);
    expect(mongoMocks.removeFDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      '/servicepath',
    );
    expect(agenda.cancel).toHaveBeenCalledWith({
      name: 'refresh-fda',
      'data.service': 'svc',
      'data.fdaId': 'fdaA',
      'data.servicePath': '/servicepath',
    });
  });

  test('deleteFDA uses normalized bucket name for object storage deletion', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo-id',
      visibility: 'private',
      servicePath: '/servicepath',
    });
    awsMocks.listObjects.mockResolvedValue(['routeTo/fdaA.parquet']);

    await deleteFDA('service_name', 'fdaA', 'private', '/servicepath');

    expect(awsMocks.listObjects).toHaveBeenCalledWith(
      {},
      'service-name',
      'servicepath/fdaA',
    );
    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'service-name', [
      'routeTo/fdaA.parquet',
    ]);
  });

  test('throws FDANotFound when FDA does not exist', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue(undefined);
    mongoMocks.retrieveFDAs.mockResolvedValue([]);

    await expect(
      deleteFDA('svc', 'missing', 'private', '/servicepath'),
    ).rejects.toMatchObject({
      status: 404,
      type: 'FDANotFound',
    });

    expect(awsMocks.getS3Client).not.toHaveBeenCalled();
  });

  test('throws FDANotFound when service is missing even if FDA exists', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo-id',
      visibility: 'private',
      servicePath: '/servicepath',
    });

    await expect(
      deleteFDA('', 'fdaA', 'private', '/servicepath'),
    ).rejects.toMatchObject({
      status: 404,
      type: 'FDANotFound',
    });

    expect(awsMocks.getS3Client).not.toHaveBeenCalled();
  });
});

describe('DA access and update helpers', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    dbMocks.getDBConnection.mockResolvedValue({});
    dbMocks.releaseDBConnection.mockResolvedValue(undefined);
    dbMocks.validateDAQuery.mockResolvedValue(undefined);
    dbMocks.checkParams.mockImplementation((params) => params);
    mongoMocks.updateDA.mockResolvedValue(undefined);
  });

  test('getDA returns DA and injects daId into response', async () => {
    mongoMocks.retrieveDA.mockResolvedValue({
      description: 'demo',
      query: 'SELECT 1',
    });

    await expect(getDA('svc', 'fdaA', 'daA')).resolves.toEqual({
      id: 'daA',
      description: 'demo',
      query: 'SELECT 1',
    });
  });

  test('getDA throws DaNotFound when DA does not exist', async () => {
    mongoMocks.retrieveDA.mockResolvedValue(undefined);

    await expect(getDA('svc', 'fdaA', 'missing')).rejects.toMatchObject({
      status: 404,
      type: 'DaNotFound',
    });
  });

  test('putDA validates and updates DA', async () => {
    await putDA('svc', 'fdaA', 'daA', 'desc', 'SELECT id', [{ name: 'id' }]);

    expect(dbMocks.checkParams).toHaveBeenCalledWith([{ name: 'id' }]);
    expect(dbMocks.validateDAQuery).toHaveBeenCalledWith(
      {},
      'svc',
      'fdaA',
      'SELECT id',
      undefined,
    );
    expect(mongoMocks.updateDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      undefined,
      'daA',
      'desc',
      'SELECT id',
      [{ name: 'id' }],
    );
    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith({});
  });

  test('putDA persists normalized params returned by checkParams', async () => {
    const normalizedParams = [
      { name: 'enabled', type: 'Boolean', default: true },
    ];
    dbMocks.checkParams.mockReturnValueOnce(normalizedParams);

    await putDA('svc', 'fdaA', 'daA', 'desc', 'SELECT id', [
      { name: 'enabled', type: 'Boolean', default: '1' },
    ]);

    expect(mongoMocks.updateDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      undefined,
      'daA',
      'desc',
      'SELECT id',
      normalizedParams,
    );
  });

  test('putDA always releases DB connection when validation fails', async () => {
    dbMocks.validateDAQuery.mockRejectedValue(new Error('invalid DA query'));

    await expect(
      putDA('svc', 'fdaA', 'daA', 'desc', 'SELECT id', []),
    ).rejects.toThrow('invalid DA query');

    expect(dbMocks.releaseDBConnection).toHaveBeenCalledWith({});
  });
});

describe('fetchFDA with refresh policies', () => {
  const agenda = {
    now: jest.fn(),
    every: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.dropFile.mockResolvedValue(undefined);
    mongoMocks.createFDAMongo.mockResolvedValue(undefined);
    dbMocks.getDBConnection.mockResolvedValue({});
    dbMocks.releaseDBConnection.mockResolvedValue(undefined);
    dbMocks.toParquet.mockResolvedValue(undefined);
    pgMocks.uploadTable.mockResolvedValue(undefined);
    jobsMocks.getAgenda.mockReturnValue(agenda);
    agenda.now.mockResolvedValue(undefined);
    agenda.every.mockResolvedValue(undefined);
  });

  test('fetchFDA with interval refresh policy schedules periodic job', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'interval',
        params: { refreshInterval: '0 0 * * *' },
      },
      'timeinstant',
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '0 0 * * *',
      'refresh-fda',
      expect.objectContaining({
        fdaId: 'fda1',
        query: 'SELECT timeinstant, 1',
        service: 'svc',
        timeColumn: 'timeinstant',
        objStgConf: undefined,
      }),
      {
        skipImmediate: true,
        unique: {
          name: 'refresh-fda',
          'data.service': 'svc',
          'data.fdaId': 'fda1',
          'data.servicePath': '/servicepath',
        },
      },
    );
  });

  test('fetchFDA with different fetch size and partition', async () => {
    dbMocks.refreshIntervalPartitionCheck.mockReturnValue(true);

    await expect(
      fetchFDA(
        'fda1',
        'SELECT 1',
        'svc',
        'public',
        '/servicepath',
        'desc',
        {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * *', // Once a day
            fetchSize: 'week',
            windowSize: 'week',
          },
        },
        'timeinstant',
        {
          partition: 'day',
        },
      ),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Fetch size "week" must be equal to partition size "day".',
    });

    expect(agenda.every).not.toHaveBeenCalled();
  });

  test('fetchFDA rejects invalid refresh policy types', async () => {
    await expect(
      fetchFDA(
        'fda1',
        'SELECT 1',
        'svc',
        'public',
        '/servicepath',
        'desc',
        {
          type: 'invalid-type',
          params: { refreshInterval: '1 hour' },
        },
        'timeinstant',
      ),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Invalid refresh policy type "invalid-type".',
    });

    expect(agenda.every).not.toHaveBeenCalled();
  });

  test('fetchFDA throws when servicePath is missing', async () => {
    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', undefined, 'desc', {
        type: 'none',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidServicePath',
    });
  });
});

describe('deleteFDA', () => {
  const agenda = {
    cancel: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jobsMocks.getAgenda.mockReturnValue(agenda);
    agenda.cancel.mockResolvedValue(undefined);
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.dropFiles.mockResolvedValue(undefined);
    mongoMocks.removeFDA.mockResolvedValue(undefined);
  });

  test('deleteFDA cancels both refresh and clean-partition scheduled jobs', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo-id',
      visibility: 'private',
      servicePath: '/servicepath',
    });
    awsMocks.listObjects.mockResolvedValue(['fda1.parquet']);

    await deleteFDA('svc', 'fda1', 'private', '/servicepath');

    expect(agenda.cancel).toHaveBeenNthCalledWith(1, {
      name: 'refresh-fda',
      'data.service': 'svc',
      'data.fdaId': 'fda1',
      'data.servicePath': '/servicepath',
    });
    expect(agenda.cancel).toHaveBeenNthCalledWith(2, {
      name: 'clean-partition',
      'data.service': 'svc',
      'data.fdaId': 'fda1',
      'data.servicePath': '/servicepath',
    });
  });
});

describe('getFDAs', () => {
  const allFdas = [
    {
      _id: 'mongo1',
      fdaId: 'fda1',
      service: 'svc',
      visibility: 'public',
      servicePath: '/public',
      query: 'SELECT 1',
      status: 'completed',
    },
    {
      _id: 'mongo2',
      fdaId: 'fda2',
      service: 'svc',
      visibility: 'private',
      servicePath: '/private',
      query: 'SELECT 2',
      status: 'completed',
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
    mongoMocks.retrieveFDAs.mockResolvedValue(allFdas);
  });

  test('returns all FDAs unfiltered when visibility and servicePath are both undefined', async () => {
    const result = await getFDAs('svc', undefined, '/public');

    expect(mongoMocks.retrieveFDAs).toHaveBeenCalledWith('svc');
    expect(result).toEqual([
      { id: 'fda1', query: 'SELECT 1', status: 'completed' },
    ]);
  });

  test('filters FDAs by visibility and servicePath when provided', async () => {
    const result = await getFDAs('svc', 'public', '/public');

    expect(result).toEqual([
      { id: 'fda1', query: 'SELECT 1', status: 'completed' },
    ]);
  });
});

describe('getFDA', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('returns stored FDA when visibility is undefined', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo1',
      fdaId: 'fdaA',
      service: 'svc',
      servicePath: '/public',
      visibility: 'private',
      query: 'SELECT 1',
      status: 'completed',
    });

    const result = await getFDA('svc', 'fdaA', undefined, '/public');

    expect(mongoMocks.retrieveFDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      '/public',
    );
    expect(result).toEqual({ query: 'SELECT 1', status: 'completed' });
  });

  test('throws FDANotFound when visibility is undefined and FDA does not exist', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue(null);

    await expect(
      getFDA('svc', 'fdaA', undefined, '/public'),
    ).rejects.toMatchObject({
      status: 404,
      type: 'FDANotFound',
    });
    expect(mongoMocks.retrieveFDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      '/public',
    );
  });

  test('throws exact stored-FDA error message and does not query candidate list', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue(undefined);

    await expect(
      getFDA('svc', 'fdaX', undefined, '/public'),
    ).rejects.toMatchObject({
      status: 404,
      type: 'FDANotFound',
      message: 'FDA fdaX not found in service svc',
    });

    expect(mongoMocks.retrieveFDAs).not.toHaveBeenCalled();
    expect(mongoMocks.retrieveFDA).toHaveBeenCalledWith(
      'svc',
      'fdaX',
      '/public',
    );
  });

  test('returns accessible FDA when visibility is provided', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue({
      _id: 'mongo1',
      fdaId: 'fdaA',
      service: 'svc',
      servicePath: '/public',
      visibility: 'public',
      query: 'SELECT 9',
      status: 'completed',
    });

    const result = await getFDA('svc', 'fdaA', 'public', '/public');

    expect(mongoMocks.retrieveFDA).toHaveBeenCalledWith(
      'svc',
      'fdaA',
      '/public',
    );
    expect(result).toEqual({ query: 'SELECT 9', status: 'completed' });
  });
});

describe('cleanPartition', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    awsMocks.getS3Client.mockReturnValue({});
    awsMocks.listObjects.mockResolvedValue([
      'svc/fdaA/2020-01-01.parquet',
      'svc/fdaA/2099-01-01.parquet',
    ]);
    awsMocks.dropFiles.mockResolvedValue(undefined);
  });

  test('drops only partitions older than the cutoff date', async () => {
    const oldDate = new Date('2020-01-01');
    const futureDate = new Date('2099-01-01');
    dbMocks.extractDate
      .mockReturnValueOnce(oldDate)
      .mockReturnValueOnce(futureDate);

    await cleanPartition(
      'svc',
      'fdaA',
      'month',
      { partition: true },
      '/public',
    );

    expect(awsMocks.listObjects).toHaveBeenCalledWith({}, 'svc', 'public/fdaA');

    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'svc', [
      'svc/fdaA/2020-01-01.parquet',
    ]);
  });

  test('cleanPartition uses normalized bucket name in object storage calls', async () => {
    dbMocks.extractDate.mockReturnValue(new Date('2099-01-01'));

    await cleanPartition(
      'service_name',
      'fdaA',
      'month',
      { partition: true },
      '/public',
    );

    expect(awsMocks.listObjects).toHaveBeenCalledWith(
      {},
      'service-name',
      'public/fdaA',
    );
    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'service-name', []);
  });

  test('calls dropFiles with empty array when no partitions are older than cutoff', async () => {
    const futureDate = new Date('2099-01-01');
    dbMocks.extractDate.mockReturnValue(futureDate);

    await cleanPartition(
      'svc',
      'fdaA',
      'month',
      { partition: true },
      '/public',
    );

    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'svc', []);
  });

  test('drops partitions older than one month when windowSize is month', async () => {
    const oldDate = new Date('2020-01-01');
    const futureDate = new Date('2099-01-01');
    dbMocks.extractDate
      .mockReturnValueOnce(oldDate)
      .mockReturnValueOnce(futureDate);

    await cleanPartition(
      'svc',
      'fdaA',
      'month',
      { partition: true },
      '/public',
    );

    expect(awsMocks.dropFiles).toHaveBeenCalledWith({}, 'svc', [
      'svc/fdaA/2020-01-01.parquet',
    ]);
  });

  test('throws CleaningError when FDA is not partitioned', async () => {
    await expect(
      cleanPartition('svc', 'fdaA', 'month', {}, '/public'),
    ).rejects.toMatchObject({
      status: 400,
      type: 'CleaningError',
    });
  });

  test('throws CleaningError when partition config is undefined', async () => {
    try {
      await cleanPartition('svc', 'fdaA', 'month', undefined, '/public');
    } catch (err) {
      expect(err).toBeInstanceOf(FDAError);
      expect(err.message).toContain('Removing a non partitioned FDA');
      expect(err.type).toContain('CleaningError');
      expect(err.status).toBe(400);
    }
  });

  test('throws CleaningError when windowSize is invalid', async () => {
    try {
      await cleanPartition(
        'svc',
        'fdaA',
        'invalid_size',
        { partition: true },
        '/public',
      );
    } catch (err) {
      expect(err).toBeInstanceOf(FDAError);
      expect(err.message).toContain('Incorrect window size in refresh policy');
      expect(err.type).toContain('CleaningError');
      expect(err.status).toBe(400);
    }
  });
});
