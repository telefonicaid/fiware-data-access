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
};

const pgMocks = {
  uploadTable: jest.fn(),
  runPgQuery: jest.fn(),
  createPgCursorReader: jest.fn(),
};

const awsMocks = {
  getS3Client: jest.fn(),
  dropFile: jest.fn(),
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
}));

await jest.unstable_mockModule('../../src/lib/utils/pg.js', () => ({
  uploadTable: pgMocks.uploadTable,
  runPgQuery: pgMocks.runPgQuery,
  createPgCursorReader: pgMocks.createPgCursorReader,
}));

await jest.unstable_mockModule('../../src/lib/utils/aws.js', () => ({
  getS3Client: awsMocks.getS3Client,
  dropFile: awsMocks.dropFile,
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

const { executeQuery, executeQueryStream, fetchFDA } = await import(
  '../../src/lib/fda.js'
);

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
    });
    dbMocks.resolveDAParams.mockReturnValue({ id: 7 });
    pgMocks.runPgQuery.mockResolvedValue([{ id: 7n }]);

    const rows = await executeQuery({
      service: 'svc',
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
        params: { fdaId: 'fdaA', daId: 'missing' },
        fresh: true,
      }),
    ).rejects.toMatchObject({ status: 404, type: 'DaNotFound' });
  });

  test('throws FDANotFound when FDA does not exist', async () => {
    mongoMocks.retrieveFDA.mockResolvedValue(undefined);

    await expect(
      executeQuery({
        service: 'svc',
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
        params: { fdaId: 'fdaA', daId: 'daA', id: 1 },
        fresh: true,
      }),
    ).rejects.toBe(pgError);
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

  test('closes cursor when request close event is triggered', async () => {
    const { req, res, reqHandlers } = createReqRes();
    const cursorReader = {
      readNextChunk: jest.fn().mockResolvedValue([]),
      close: jest.fn().mockResolvedValue(undefined),
    };
    pgMocks.createPgCursorReader.mockResolvedValue(cursorReader);

    await executeQueryStream({
      service: 'svc',
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
    await fetchFDA('fda1', 'SELECT id FROM users;', 'svc', '/svc', 'test FDA', {
      type: 'none',
    });

    expect(mongoMocks.createFDAMongo).toHaveBeenCalledWith(
      'fda1',
      'SELECT id FROM users;',
      'svc',
      '/svc',
      'test FDA',
      { type: 'none' },
    );
    expect(pgMocks.uploadTable).toHaveBeenCalledWith(
      {},
      'svc',
      'svc',
      'SELECT * FROM (SELECT id FROM users) AS fda_one_row LIMIT 1',
      'fda1',
    );
    expect(dbMocks.toParquet).toHaveBeenCalledWith(
      {},
      'svc/fda1.csv',
      'svc/fda1.parquet',
    );
    expect(awsMocks.dropFile).toHaveBeenCalledWith({}, 'svc', 'fda1.csv');
    expect(agenda.now).toHaveBeenCalledWith('refresh-fda', {
      fdaId: 'fda1',
      query: 'SELECT id FROM users;',
      service: 'svc',
    });
    expect(agenda.every).not.toHaveBeenCalled();
  });

  test('schedules periodic refresh for interval policy', async () => {
    await fetchFDA('fda1', 'SELECT 1', 'svc', '/svc', 'desc', {
      type: 'interval',
      value: '10 minutes',
    });

    expect(agenda.every).toHaveBeenCalledWith(
      '10 minutes',
      'refresh-fda',
      { fdaId: 'fda1', query: 'SELECT 1', service: 'svc' },
      { unique: { name: 'refresh-fda', 'data.fdaId': 'fda1' } },
    );
  });

  test('rolls back FDA provisioning and rethrows when parquet creation fails', async () => {
    const uploadError = new Error('S3 unreachable');
    pgMocks.uploadTable.mockRejectedValue(uploadError);

    await expect(
      fetchFDA('fda1', 'SELECT 1', 'svc', '/svc', 'desc', { type: 'none' }),
    ).rejects.toBe(uploadError);

    expect(mongoMocks.removeFDA).toHaveBeenCalledWith('svc', 'fda1');
    expect(awsMocks.dropFile).toHaveBeenCalledWith({}, 'svc', 'fda1.csv');
    expect(awsMocks.dropFile).toHaveBeenCalledWith({}, 'svc', 'fda1.parquet');
    expect(agenda.now).not.toHaveBeenCalled();
  });
});
