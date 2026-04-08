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
import request from 'supertest';

const fetcherMocks = {
  startFetcher: jest.fn(),
};

const jobsMocks = {
  shutdownAgenda: jest.fn(),
  initAgenda: jest.fn(),
};

const fdaMocks = {
  getFDAs: jest.fn(),
  fetchFDA: jest.fn(),
  executeQuery: jest.fn(),
  executeQueryStream: jest.fn(),
  createDA: jest.fn(),
  getFDA: jest.fn(),
  updateFDA: jest.fn(),
  deleteFDA: jest.fn(),
  getDAs: jest.fn(),
  getDA: jest.fn(),
  putDA: jest.fn(),
  deleteDA: jest.fn(),
};

const mongoMocks = {
  createIndex: jest.fn(),
  disconnectClient: jest.fn(),
  getOperationalCollectionsSnapshot: jest.fn(),
};

const awsMocks = {
  destroyS3Client: jest.fn(),
};

const pgUtilsMocks = {
  closePgPools: jest.fn(),
};

const loggerMock = {
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
};

const initialLoggerMock = {
  fatal: jest.fn(),
};

const cdaMocks = {
  handleCdaQuery: jest.fn(),
};

const utilsMocks = {
  validateAllowedFieldsBody: jest.fn(),
  parseBooleanQueryParam: jest.fn(),
};

const originalNodeEnv = process.env.NODE_ENV;

function resetModuleMocks() {
  fetcherMocks.startFetcher.mockReset().mockResolvedValue(undefined);

  jobsMocks.shutdownAgenda.mockReset().mockResolvedValue(undefined);
  jobsMocks.initAgenda.mockReset().mockResolvedValue(undefined);

  fdaMocks.getFDAs.mockReset().mockResolvedValue([]);
  fdaMocks.fetchFDA.mockReset().mockResolvedValue(undefined);
  fdaMocks.executeQuery.mockReset().mockResolvedValue([{ ok: true }]);
  fdaMocks.executeQueryStream.mockReset().mockImplementation(({ res }) => {
    res.status(200).send('streamed');
  });
  fdaMocks.createDA.mockReset().mockResolvedValue(undefined);
  fdaMocks.getFDA.mockReset().mockResolvedValue({ fdaId: 'fda1' });
  fdaMocks.updateFDA.mockReset().mockResolvedValue(undefined);
  fdaMocks.deleteFDA.mockReset().mockResolvedValue(undefined);
  fdaMocks.getDAs.mockReset().mockResolvedValue([]);
  fdaMocks.getDA.mockReset().mockResolvedValue({ id: 'da1' });
  fdaMocks.putDA.mockReset().mockResolvedValue(undefined);
  fdaMocks.deleteDA.mockReset().mockResolvedValue(undefined);

  mongoMocks.createIndex.mockReset().mockResolvedValue(undefined);
  mongoMocks.disconnectClient.mockReset().mockResolvedValue(undefined);
  mongoMocks.getOperationalCollectionsSnapshot.mockReset().mockResolvedValue({
    fdasTotal: 2,
    dasTotal: 5,
    fdasByStatus: [
      { status: 'completed', count: 1 },
      { status: 'failed', count: 1 },
    ],
    fdasByServiceAndPath: [
      { service: 'svc', servicePath: '/servicepath', count: 2 },
    ],
    agenda: {
      total: 3,
      failed: 1,
      locked: 0,
      byName: [{ name: 'refresh-fda', count: 3 }],
    },
  });

  awsMocks.destroyS3Client.mockReset().mockResolvedValue(undefined);
  pgUtilsMocks.closePgPools.mockReset().mockResolvedValue(undefined);

  loggerMock.info.mockReset();
  loggerMock.warn.mockReset();
  loggerMock.error.mockReset();
  loggerMock.debug.mockReset();
  initialLoggerMock.fatal.mockReset();

  cdaMocks.handleCdaQuery.mockReset().mockResolvedValue({ rows: [] });

  utilsMocks.validateAllowedFieldsBody.mockReset().mockReturnValue(undefined);
  utilsMocks.parseBooleanQueryParam.mockReset().mockReturnValue(false);
}

async function flushAsyncWork() {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
}

async function loadIndexModule({
  nodeEnv = 'test',
  roles = { apiServer: true, fetcher: true },
  createIndexError,
  mongoSnapshotError,
  startFetcherError,
  initAgendaError,
  disconnectError,
  destroyS3Error,
  mockListen = false,
} = {}) {
  jest.resetModules();
  resetModuleMocks();

  process.env.NODE_ENV = nodeEnv;

  if (createIndexError) {
    mongoMocks.createIndex.mockRejectedValueOnce(createIndexError);
  }

  if (mongoSnapshotError) {
    mongoMocks.getOperationalCollectionsSnapshot.mockRejectedValue(
      mongoSnapshotError,
    );
  }

  if (startFetcherError) {
    fetcherMocks.startFetcher.mockRejectedValueOnce(startFetcherError);
  }

  if (initAgendaError) {
    jobsMocks.initAgenda.mockRejectedValueOnce(initAgendaError);
  }

  if (disconnectError) {
    mongoMocks.disconnectClient.mockRejectedValueOnce(disconnectError);
  }

  if (destroyS3Error) {
    awsMocks.destroyS3Client.mockRejectedValueOnce(destroyS3Error);
  }

  let appListenSpy;
  if (mockListen) {
    const expressModule = await import('express');
    appListenSpy = jest
      .spyOn(expressModule.default.application, 'listen')
      .mockImplementation(function (...args) {
        const callback = args[1];
        if (typeof callback === 'function') {
          callback();
        }

        return { close: jest.fn() };
      });
  }

  const registeredSignals = new Map();
  const processOnSpy = jest
    .spyOn(process, 'on')
    .mockImplementation((signal, handler) => {
      registeredSignals.set(signal, handler);
      return process;
    });
  const processExitSpy = jest
    .spyOn(process, 'exit')
    .mockImplementation(() => undefined);

  await jest.unstable_mockModule('../../src/fetcher.js', () => ({
    startFetcher: fetcherMocks.startFetcher,
  }));

  await jest.unstable_mockModule('../../src/lib/jobs.js', () => ({
    shutdownAgenda: jobsMocks.shutdownAgenda,
    initAgenda: jobsMocks.initAgenda,
  }));

  await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
    VALID_VISIBILITIES: ['public', 'private'],
    getFDAs: fdaMocks.getFDAs,
    fetchFDA: fdaMocks.fetchFDA,
    executeQuery: fdaMocks.executeQuery,
    executeQueryStream: fdaMocks.executeQueryStream,
    assertFDAAccess: jest.fn(),
    createDA: fdaMocks.createDA,
    getFDA: fdaMocks.getFDA,
    updateFDA: fdaMocks.updateFDA,
    deleteFDA: fdaMocks.deleteFDA,
    getDAs: fdaMocks.getDAs,
    getDA: fdaMocks.getDA,
    putDA: fdaMocks.putDA,
    deleteDA: fdaMocks.deleteDA,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
    createIndex: mongoMocks.createIndex,
    disconnectClient: mongoMocks.disconnectClient,
    getOperationalCollectionsSnapshot:
      mongoMocks.getOperationalCollectionsSnapshot,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/aws.js', () => ({
    destroyS3Client: awsMocks.destroyS3Client,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/pg.js', () => ({
    closePgPools: pgUtilsMocks.closePgPools,
  }));

  await jest.unstable_mockModule('../../src/lib/fdaConfig.js', () => ({
    config: {
      port: 0,
      logger: { resSize: 120 },
      roles,
    },
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    initLogger: jest.fn(),
    getBasicLogger: () => loggerMock,
    getInitialLogger: () => initialLoggerMock,
  }));

  await jest.unstable_mockModule('../../src/lib/compat/cdaAdapter.js', () => ({
    handleCdaQuery: cdaMocks.handleCdaQuery,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/utils.js', () => ({
    validateAllowedFieldsBody: utilsMocks.validateAllowedFieldsBody,
    parseBooleanQueryParam: utilsMocks.parseBooleanQueryParam,
  }));

  const mod = await import('../../src/index.js');
  await flushAsyncWork();

  return {
    app: mod.app,
    registeredSignals,
    processOnSpy,
    processExitSpy,
    appListenSpy,
  };
}

describe('index routes - validation and middleware branches', () => {
  let app;

  beforeEach(async () => {
    ({ app } = await loadIndexModule({
      nodeEnv: 'test',
      roles: { apiServer: true, fetcher: true },
    }));
  });

  afterEach(() => {
    jest.restoreAllMocks();
    process.env.NODE_ENV = originalNodeEnv;
  });

  test('returns 400 for missing mandatory params across route guards', async () => {
    await request(app).get('/public/fdas').expect(400);
    await request(app).post('/public/fdas').send({}).expect(400);
    await request(app).get('/public/fdas/fda1').expect(400);
    await request(app).put('/public/fdas/fda1').expect(400);
    await request(app).delete('/public/fdas/fda1').expect(400);

    await request(app).get('/public/fdas/fda1/das').expect(400);
    await request(app).post('/public/fdas/fda1/das').send({}).expect(400);
    await request(app).get('/public/fdas/fda1/das/da1').expect(400);
    await request(app)
      .put('/public/fdas/fda1/das/da1')
      .set('Fiware-Service', 'svc')
      .send({ description: 'without query' })
      .expect(400);

    await request(app).delete('/public/fdas/fda1/das/da1').expect(400);

    await request(app).get('/public/fdas/fda1/das/da1/data').expect(400);
    await request(app).post('/plugin/cda/api/doQuery').send({}).expect(400);
  });

  test('covers health and successful CRUD-like route flows', async () => {
    fdaMocks.getFDAs.mockResolvedValueOnce([
      { id: 'fda1', visibility: 'public', servicePath: '/servicepath' },
    ]);
    fdaMocks.getFDA.mockResolvedValueOnce({ id: 'fda1' });
    fdaMocks.getDAs.mockResolvedValueOnce([{ id: 'da1' }]);
    fdaMocks.getDA.mockResolvedValueOnce({ id: 'da1' });

    const healthRes = await request(app).get('/health').expect(200);
    expect(healthRes.body.status).toBe('UP');
    expect(healthRes.body).toEqual(
      expect.objectContaining({
        timestamp: expect.any(String),
        uptimeSeconds: expect.any(Number),
        process: expect.objectContaining({
          pid: expect.any(Number),
          nodeVersion: expect.any(String),
          memory: expect.objectContaining({
            rssBytes: expect.any(Number),
            heapTotalBytes: expect.any(Number),
            heapUsedBytes: expect.any(Number),
          }),
        }),
        roles: expect.objectContaining({
          apiServer: expect.any(Boolean),
          fetcher: expect.any(Boolean),
          syncQueries: expect.any(Boolean),
        }),
        traffic: expect.objectContaining({
          totalRequests: expect.any(Number),
          errorRequests: expect.any(Number),
          inFlightRequests: expect.any(Number),
          routesObserved: expect.any(Number),
        }),
        fiware: expect.objectContaining({
          requestsWithHeaders: expect.any(Number),
          servicesObserved: expect.any(Number),
          servicePathsObserved: expect.any(Number),
        }),
      }),
    );

    await request(app)
      .get('/public/fdas')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(200)
      .expect([
        { id: 'fda1', visibility: 'public', servicePath: '/servicepath' },
      ]);

    await request(app)
      .post('/public/fdas')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ id: 'fda1', query: 'SELECT 1', description: 'desc' })
      .expect(202);

    await request(app)
      .get('/public/fdas/fda1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(200)
      .expect({ id: 'fda1' });

    await request(app)
      .put('/public/fdas/fda1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({})
      .expect(202);

    await request(app)
      .delete('/public/fdas/fda1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(204);

    await request(app)
      .get('/public/fdas/fda1/das')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(200)
      .expect([{ id: 'da1' }]);

    await request(app)
      .post('/public/fdas/fda1/das')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ id: 'da1', query: 'SELECT 2', description: 'da' })
      .expect(201);

    await request(app)
      .get('/public/fdas/fda1/das/da1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(200)
      .expect({ id: 'da1' });

    await request(app)
      .put('/public/fdas/fda1/das/da1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ query: 'SELECT 3', description: 'new' })
      .expect(204);

    await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .query({ fresh: 'false', limit: 10 })
      .expect(200);

    await request(app)
      .post('/plugin/cda/api/doQuery')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ path: '/public/svc', dataAccessId: 'da1' })
      .expect(200)
      .expect({ rows: [] });

    expect(fdaMocks.fetchFDA).toHaveBeenCalledWith(
      'fda1',
      'SELECT 1',
      'svc',
      'public',
      '/servicepath',
      'desc',
      { type: 'none' },
      undefined,
      {},
    );
    expect(fdaMocks.updateFDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      'public',
      '/servicepath',
    );
    expect(fdaMocks.deleteFDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      'public',
      '/servicepath',
    );
    expect(fdaMocks.createDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      'da1',
      'da',
      'SELECT 2',
      undefined,
      'public',
      '/servicepath',
    );
    expect(fdaMocks.putDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      'da1',
      'new',
      'SELECT 3',
      undefined,
      'public',
      '/servicepath',
    );
    expect(utilsMocks.validateAllowedFieldsBody).toHaveBeenCalled();
  });

  test('returns 400 when PUT /fdas receives a body payload', async () => {
    const res = await request(app)
      .put('/public/fdas/fda1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ invalid: true })
      .expect(400);

    expect(res.body).toEqual({
      error: 'BadRequest',
      description: 'PUT /fdas does not accept a request body',
    });
  });

  test('returns prometheus metrics in text format by default', async () => {
    await request(app).get('/health').expect(200);

    const res = await request(app).get('/metrics').expect(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.headers['content-type']).toContain('version=0.0.4');
    expect(res.headers['content-type']).toContain('charset=utf-8');
    expect(res.text).toContain(
      '# HELP fda_up Service liveness indicator (1=up).',
    );
    expect(res.text).toContain('# TYPE fda_http_server_requests_total counter');
    expect(res.text).toContain('fda_http_server_requests_total');
    expect(res.text).toContain('fda_tenant_requests_total');
    expect(res.text).toContain('fda_catalog_fdas_by_service');
    expect(res.text).toContain('fda_jobs_agenda_total');
    expect(res.text).toContain('# EOF');
  });

  test('health payload includes mongo operational snapshot fields', async () => {
    const res = await request(app).get('/health').expect(200);

    expect(res.body.mongo).toEqual(
      expect.objectContaining({
        scrapeOk: true,
        source: expect.stringMatching(/live|cache/),
        fdasTotal: 2,
        dasTotal: 5,
        agendaJobsTotal: 3,
        agendaJobsFailed: 1,
        agendaJobsLocked: 0,
      }),
    );
  });

  test('metrics still respond when mongo snapshot fails and expose scrape failure metric', async () => {
    ({ app } = await loadIndexModule({
      nodeEnv: 'test',
      roles: { apiServer: true, fetcher: true },
      mongoSnapshotError: new Error('mongo offline'),
    }));

    const healthRes = await request(app).get('/health').expect(200);
    expect(healthRes.body.mongo.scrapeOk).toBe(false);
    expect(healthRes.body.mongo.source).toBe('stale');
    expect(healthRes.body.mongo.lastError).toContain('mongo offline');

    const metricsRes = await request(app).get('/metrics').expect(200);
    expect(metricsRes.text).toContain('fda_mongo_scrape_success 0');
    expect(metricsRes.text).toContain('# EOF');
  });

  test('returns openmetrics content-type when requested by Accept header', async () => {
    const res = await request(app)
      .get('/metrics')
      .set('Accept', 'application/openmetrics-text')
      .expect(200);

    expect(res.headers['content-type']).toContain(
      'application/openmetrics-text',
    );
    expect(res.headers['content-type']).toContain('version=1.0.0');
    expect(res.headers['content-type']).toContain('charset=utf-8');
    expect(res.text).toContain('# TYPE fda_up gauge');
    expect(res.text).toContain('# EOF');
  });

  test('returns 406 when Accept header does not include a supported metrics format', async () => {
    const res = await request(app)
      .get('/metrics')
      .set('Accept', 'application/json')
      .expect(406);

    expect(res.body).toEqual({
      error: 'NotAcceptable',
      description:
        'Accept header must allow application/openmetrics-text or text/plain',
    });
  });

  test('routes data endpoint through NDJSON streaming when Accept requests ndjson', async () => {
    await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .set('Accept', 'application/x-ndjson')
      .query({ fresh: 'true', offset: 2 })
      .expect(200)
      .expect('streamed');

    expect(fdaMocks.executeQueryStream).toHaveBeenCalledWith(
      expect.objectContaining({
        service: 'svc',
        visibility: 'public',
        servicePath: '/servicepath',
        params: expect.objectContaining({
          fdaId: 'fda1',
          daId: 'da1',
          offset: '2',
        }),
        fresh: false,
      }),
    );
    expect(fdaMocks.executeQuery).not.toHaveBeenCalled();
  });

  test('uses NDJSON streaming when Accept is ndjson even if outputType is not json', async () => {
    await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .set('Accept', 'application/x-ndjson')
      .query({
        outputType: 'csv',
        minAge: 25,
      })
      .expect(200)
      .expect('streamed');

    expect(fdaMocks.executeQueryStream).toHaveBeenCalledWith(
      expect.objectContaining({
        service: 'svc',
        visibility: 'public',
        servicePath: '/servicepath',
        params: expect.objectContaining({
          fdaId: 'fda1',
          daId: 'da1',
          minAge: '25',
        }),
      }),
    );
    expect(fdaMocks.executeQuery).not.toHaveBeenCalled();
  });

  test('covers DELETE DA route success path', async () => {
    await request(app)
      .delete('/public/fdas/fda1/das/da1')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(204);

    expect(fdaMocks.deleteDA).toHaveBeenCalledWith(
      'svc',
      'fda1',
      'da1',
      'public',
      '/servicepath',
    );
  });

  test('handles CDA adapter exceptions with a 500 response', async () => {
    cdaMocks.handleCdaQuery.mockRejectedValueOnce(
      new Error('adapter exploded'),
    );

    const res = await request(app)
      .post('/plugin/cda/api/doQuery')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ path: '/public/svc', dataAccessId: 'da1' });

    expect(res.status).toBe(500);
    expect(res.body).toEqual({
      error: 'InternalServerError',
      description: 'adapter exploded',
    });
    expect(loggerMock.error).toHaveBeenCalledWith(
      'Error executing query:',
      expect.any(Error),
    );
  });

  test('covers middleware capture fallback for unserializable payloads', async () => {
    fdaMocks.executeQuery.mockResolvedValueOnce([{ total: 1n }]);

    const res = await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .query({});

    expect(res.status).toBe(500);
    expect(res.body.error).toBe('InternalServerError');
  });

  test('uses error middleware 500 branch when route throws generic errors', async () => {
    fdaMocks.getFDAs.mockRejectedValueOnce(new Error('unexpected failure'));

    const res = await request(app)
      .get('/public/fdas')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(500);

    expect(res.body).toEqual({
      error: 'InternalServerError',
      description: 'unexpected failure',
    });
    expect(loggerMock.error).toHaveBeenCalledWith(expect.any(Error));
  });

  test('uses error middleware warning branch for handled 4xx errors', async () => {
    const err = new Error('missing fda');
    err.status = 404;
    err.type = 'NotFound';
    fdaMocks.getFDAs.mockRejectedValueOnce(err);

    const res = await request(app)
      .get('/public/fdas')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(404);

    expect(res.body).toEqual({
      error: 'NotFound',
      description: 'missing fda',
    });
    expect(loggerMock.warn).toHaveBeenCalledWith(expect.any(Error));
  });

  test('returns 400 for invalid outputType on data endpoint', async () => {
    const res = await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .query({ outputType: 'xml' })
      .expect(400);

    expect(res.body).toEqual({
      error: 'BadRequest',
      description: expect.stringContaining("Invalid outputType 'xml'"),
    });
    expect(fdaMocks.executeQuery).not.toHaveBeenCalled();
  });

  test('returns CSV when outputType=csv is requested on data endpoint', async () => {
    fdaMocks.executeQuery.mockResolvedValueOnce([
      { col1: 'a', col2: 'b' },
      { col1: 'c,d', col2: 'e"f' },
    ]);

    const res = await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .query({ outputType: 'csv' })
      .expect(200);

    expect(res.headers['content-type']).toMatch(/text\/csv/);
    expect(res.headers['content-disposition']).toMatch(/attachment/);
    const lines = res.text.split('\n');
    expect(lines[0]).toBe('col1,col2');
    expect(lines[1]).toBe('a,b');
    expect(lines[2]).toBe('"c,d","e""f"');
    expect(fdaMocks.executeQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        service: 'svc',
        visibility: 'public',
        servicePath: '/servicepath',
        params: expect.not.objectContaining({ outputType: 'csv' }),
      }),
    );
  });

  test('returns Excel buffer when outputType=xls is requested on data endpoint', async () => {
    fdaMocks.executeQuery.mockResolvedValueOnce([{ col1: 'v1', col2: 42 }]);

    const res = await request(app)
      .get('/public/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .query({ outputType: 'xls' })
      .expect(200);

    expect(res.headers['content-type']).toMatch(
      /application\/vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet/,
    );
    expect(res.headers['content-disposition']).toMatch(/results\.xlsx/);
    expect(res.body).toBeTruthy();
  });

  test('returns 400 for invalid visibility on data endpoint', async () => {
    const invalidVisibilityError = new Error(
      'Visibility must be public or private',
    );
    invalidVisibilityError.status = 400;
    invalidVisibilityError.type = 'InvalidVisibility';
    fdaMocks.executeQuery.mockRejectedValueOnce(invalidVisibilityError);

    const res = await request(app)
      .get('/shared/fdas/fda1/das/da1/data')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .expect(400);

    expect(res.body).toEqual({
      error: 'InvalidVisibility',
      description: 'Visibility must be public or private',
    });
  });

  test('returns 400 for invalid Fiware-ServicePath when creating FDAs', async () => {
    const invalidServicePathError = new Error(
      'Fiware-ServicePath must be a non-root absolute path (e.g. /servicepath)',
    );
    invalidServicePathError.status = 400;
    invalidServicePathError.type = 'InvalidServicePath';
    fdaMocks.fetchFDA.mockRejectedValueOnce(invalidServicePathError);

    const res = await request(app)
      .post('/public/fdas')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', 'shared')
      .send({ id: 'fda1', query: 'SELECT 1' })
      .expect(400);

    expect(res.body).toEqual({
      error: 'InvalidServicePath',
      description:
        'Fiware-ServicePath must be a non-root absolute path (e.g. /servicepath)',
    });
  });

  test('returns 400 for invalid outputType on POST /doQuery', async () => {
    const res = await request(app)
      .post('/plugin/cda/api/doQuery')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ path: '/public/svc', dataAccessId: 'da1', outputType: 'html' })
      .expect(400);

    expect(res.body).toEqual({
      error: 'BadRequest',
      description: expect.stringContaining("Invalid outputType 'html'"),
    });
    expect(cdaMocks.handleCdaQuery).not.toHaveBeenCalled();
  });

  test('returns CSV when outputType=csv is requested on POST /doQuery', async () => {
    cdaMocks.handleCdaQuery.mockResolvedValueOnce([{ col1: 'x', col2: 'y' }]);

    const res = await request(app)
      .post('/plugin/cda/api/doQuery')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ path: '/public/svc', dataAccessId: 'da1', outputType: 'csv' })
      .expect(200);

    expect(res.headers['content-type']).toMatch(/text\/csv/);
    expect(res.text).toContain('col1,col2');
    expect(cdaMocks.handleCdaQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        outputType: 'csv',
      }),
    );
  });

  test('returns Excel buffer when outputType=xls is requested on POST /doQuery', async () => {
    cdaMocks.handleCdaQuery.mockResolvedValueOnce([{ col1: 'v1' }]);

    const res = await request(app)
      .post('/plugin/cda/api/doQuery')
      .set('Fiware-Service', 'svc')
      .set('Fiware-ServicePath', '/servicepath')
      .send({ path: '/public/svc', dataAccessId: 'da1', outputType: 'xls' })
      .expect(200);

    expect(res.headers['content-type']).toMatch(
      /application\/vnd\.openxmlformats-officedocument\.spreadsheetml\.sheet/,
    );
    expect(res.headers['content-disposition']).toMatch(/results\.xlsx/);
  });
});

describe('index bootstrap and shutdown branches', () => {
  afterEach(() => {
    jest.restoreAllMocks();
    process.env.NODE_ENV = originalNodeEnv;
  });

  test('logs startup failure and exits when no role is enabled', async () => {
    const { processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: false, fetcher: false },
    });

    expect(loggerMock.error).toHaveBeenCalledWith(
      expect.stringContaining('Startup failed: Error: At least one FDA role'),
    );
    expect(processExitSpy).toHaveBeenCalledWith(1);
  });

  test('logs startup failure and exits when createIndex rejects', async () => {
    const { processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: true, fetcher: false },
      createIndexError: new Error('mongo down'),
    });

    expect(loggerMock.error).toHaveBeenCalledWith(
      expect.stringContaining('Startup failed: Error: mongo down'),
    );
    expect(processExitSpy).toHaveBeenCalledWith(1);
  });

  test('handles startFetcher rejection branch during startup', async () => {
    const { processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: false, fetcher: true },
      startFetcherError: new Error('fetcher down'),
    });

    expect(loggerMock.error).toHaveBeenCalledWith(
      '[Fetcher] Failed to start',
      expect.any(Error),
    );
    expect(processExitSpy).toHaveBeenCalledWith(1);
  });

  test('shutdown returns early when already shutting down', async () => {
    const { registeredSignals, processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: false, fetcher: true },
    });

    const sigtermHandler = registeredSignals.get('SIGTERM');

    await sigtermHandler();
    await sigtermHandler();

    expect(mongoMocks.disconnectClient).toHaveBeenCalledTimes(1);
    expect(awsMocks.destroyS3Client).toHaveBeenCalledTimes(1);
    expect(pgUtilsMocks.closePgPools).toHaveBeenCalledTimes(1);
    expect(processExitSpy).toHaveBeenCalledWith(0);
  });

  test('shutdown logs error branch and exits with code 1 when cleanup fails', async () => {
    const { registeredSignals, processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: false, fetcher: true },
      disconnectError: new Error('disconnect fail'),
    });

    const sigintHandler = registeredSignals.get('SIGINT');
    await sigintHandler();

    expect(loggerMock.error).toHaveBeenCalledWith(
      '[SHUTDOWN] Failed',
      expect.any(Error),
    );
    expect(processExitSpy).toHaveBeenCalledWith(1);
  });

  test('logs startup failure and exits when initAgenda rejects', async () => {
    const { processExitSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: true, fetcher: false },
      initAgendaError: new Error('agenda down'),
    });

    expect(loggerMock.error).toHaveBeenCalledWith(
      expect.stringContaining('Startup failed: Error: agenda down'),
    );
    expect(processExitSpy).toHaveBeenCalledWith(1);
  });

  test('starts API listener when apiServer role is enabled', async () => {
    const { appListenSpy } = await loadIndexModule({
      nodeEnv: 'integration',
      roles: { apiServer: true, fetcher: false },
      mockListen: true,
    });

    expect(appListenSpy).toHaveBeenCalledWith(0, expect.any(Function));
    expect(loggerMock.debug).toHaveBeenCalledWith(
      expect.stringContaining('API Server listening at port 0'),
    );
  });
});
