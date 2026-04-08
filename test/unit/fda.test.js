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
  executeQueryStream,
  executeQueryCsvStream,
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

    await executeQueryCsvStream({
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

    await executeQueryCsvStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
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

    await executeQueryCsvStream({
      service: 'svc',
      visibility: 'private',
      servicePath: '/servicepath',
      params: { fdaId: 'fdaA', daId: 'daA' },
      req,
      res,
      fresh: false,
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
      executeQueryCsvStream({
        service: 'svc',
        visibility: 'private',
        servicePath: '/servicepath',
        params: { fdaId: 'fdaA', daId: 'daA' },
        req,
        res,
        fresh: false,
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
      objStgConf: undefined,
    });
    expect(agenda.every).not.toHaveBeenCalled();
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
        value: '10 minutes',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '10 minutes',
      'refresh-fda',
      {
        fdaId: 'fda1',
        query: 'SELECT 1',
        service: 'svc',
        servicePath: '/servicepath',
        timeColumn: undefined,
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

  test('fetchFDA with cron refresh policy schedules periodic job', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'cron',
        value: '0 0 * * *',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '0 0 * * *',
      'refresh-fda',
      expect.objectContaining({ fdaId: 'fda1', query: 'SELECT 1' }),
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

  test('fetchFDA with window refresh policy and deleteInterval schedules cleanup', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'window',
        value: 'daily',
        deleteInterval: '1 day',
        windowSize: 'day',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      expect.any(String),
      'refresh-fda',
      expect.any(Object),
      expect.any(Object),
    );
  });

  test('fetchFDA throws when deleteInterval provided without windowSize', async () => {
    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', '/servicepath', 'desc', {
        type: 'interval',
        value: '10 minutes',
        deleteInterval: '1 day',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Window size is required with a delete interval.',
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
});

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

  test('fetchFDA with cron refresh policy schedules periodic job', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'cron',
        value: '0 0 * * *',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      '0 0 * * *',
      'refresh-fda',
      expect.objectContaining({ fdaId: 'fda1', query: 'SELECT 1' }),
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

  test('fetchFDA with window refresh policy and deleteInterval schedules cleanup', async () => {
    await fetchFDA(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      {
        type: 'window',
        value: 'daily',
        deleteInterval: '1 day',
        windowSize: 'day',
      },
    );

    expect(agenda.every).toHaveBeenCalledWith(
      expect.any(String),
      'refresh-fda',
      expect.any(Object),
      expect.any(Object),
    );
  });

  test('fetchFDA throws when deleteInterval provided without windowSize', async () => {
    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', 'public', '/servicepath', 'desc', {
        type: 'interval',
        value: '10 minutes',
        deleteInterval: '1 day',
      }),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Window size is required with a delete interval.',
    });
  });

  test('fetchFDA with window policy throws when deleteInterval provided without windowSize', async () => {
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
          value: 'daily',
          deleteInterval: '1 day',
        },
        'timeinstant',
      ),
    ).rejects.toMatchObject({
      status: 400,
      type: 'InvalidParam',
      message: 'Window size is required with a delete interval.',
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

  test('throws CleaningError when FDA is not partitioned', async () => {
    await expect(
      cleanPartition('svc', 'fdaA', 'month', {}, '/public'),
    ).rejects.toMatchObject({
      status: 400,
      type: 'CleaningError',
    });
  });

  test('throws CleaningError when partition config is undefined', async () => {
    await expect(
      cleanPartition('svc', 'fdaA', 'month', undefined, '/public'),
    ).rejects.toMatchObject({
      status: 400,
      type: 'CleaningError',
    });
  });

  test('throws CleaningError when windowSize is invalid', async () => {
    await expect(
      cleanPartition(
        'svc',
        'fdaA',
        'invalid_size',
        { partition: true },
        '/public',
      ),
    ).rejects.toMatchObject({
      status: 400,
      type: 'CleaningError',
    });
  });
});
