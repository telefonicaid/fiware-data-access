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

let currentClient;

const clientCtorMock = jest.fn(function clientFactory() {
  return currentClient;
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
      usr: 'pg-user',
      pass: 'pg-pass',
      host: 'pg-host',
      port: 5432,
    },
  },
}));

const { getPgClient, uploadTable, runPgQuery, createPgCursorReader } =
  await import('../../src/lib/utils/pg.js');

describe('pg utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    currentClient = {
      connect: jest.fn().mockResolvedValue(undefined),
      query: jest.fn(),
      end: jest.fn().mockResolvedValue(undefined),
    };
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

    const rows = await runPgQuery('svc_db', 'SELECT 1', []);

    expect(currentClient.connect).toHaveBeenCalledTimes(1);
    expect(currentClient.query).toHaveBeenCalledWith('SELECT 1', []);
    expect(rows).toEqual([{ id: 1 }]);
  });

  test('runPgQuery wraps non-FDA errors and swallows end failures', async () => {
    currentClient.query.mockRejectedValue(new Error('query exploded'));
    currentClient.end.mockRejectedValue(new Error('close exploded'));

    await expect(
      runPgQuery('svc_db', 'SELECT bad()', []),
    ).rejects.toMatchObject({
      status: 500,
      type: 'PostgresServerError',
      message: 'Error running fresh query: query exploded',
    });

    expect(currentClient.end).toHaveBeenCalledTimes(1);
  });

  test('runPgQuery rethrows FDAError instances', async () => {
    const fdaError = new FDAError(429, 'TooManyFreshQueries', 'limit');
    currentClient.query.mockRejectedValue(fdaError);

    await expect(runPgQuery('svc_db', 'SELECT 1', [])).rejects.toBe(fdaError);
  });

  test('createPgCursorReader reads chunk and closes idempotently', async () => {
    const cursorObject = {
      read: jest.fn((size, callback) => callback(null, [{ id: size }])),
      close: jest.fn((callback) => callback(null)),
    };
    currentClient.query.mockReturnValue(cursorObject);

    const reader = await createPgCursorReader('svc_db', 'SELECT id', [9], 100);

    await expect(reader.readNextChunk()).resolves.toEqual([{ id: 100 }]);

    await reader.close();
    await reader.close();

    expect(cursorCtorMock).toHaveBeenCalledWith('SELECT id', [9]);
    expect(cursorObject.close).toHaveBeenCalledTimes(1);
    expect(currentClient.end).toHaveBeenCalledTimes(1);
  });

  test('createPgCursorReader wraps non-FDA errors', async () => {
    currentClient.connect.mockRejectedValue(new Error('connect exploded'));
    currentClient.end.mockRejectedValue(new Error('close exploded'));

    await expect(
      createPgCursorReader('svc_db', 'SELECT 1', [], 10),
    ).rejects.toMatchObject({
      status: 500,
      type: 'PostgresServerError',
      message: 'Error streaming fresh query: connect exploded',
    });

    expect(currentClient.end).toHaveBeenCalledTimes(1);
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

    await expect(
      uploadTable({}, 'bucketA', 'svc_db', 'SELECT * FROM t', 'path/file'),
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
    expect(currentClient.end).toHaveBeenCalledTimes(1);
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

    await uploadTable({}, 'bucketA', 'svc_db', 'SELECT * FROM t', 'path/file');

    expect(loggerMock.debug).toHaveBeenCalledWith(
      'Upload completed successfully',
    );
    expect(stream.destroy).toHaveBeenCalledTimes(1);
    expect(currentClient.end).toHaveBeenCalledTimes(1);
  });
});
