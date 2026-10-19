import { jest, test, expect } from '@jest/globals';
import { waitUntilFDAStatus } from '../performance/utils/performanceTestUtils.js';

const options = {
  baseUrl: 'http://localhost:1234',
  service: 'traffic',
  servicePath: '/fdas',
  visibility: 'private',
  fdaId: 'traffic-window',
  status: 'completed',
  timeout: 100,
};

test('performance polling reports OOM immediately, even when progress exceeds the target', async () => {
  const httpReq = jest.fn().mockResolvedValue({
    status: 200,
    json: {
      status: 'failed',
      progress: 100,
      error: 'Out of Memory Error: could not allocate block of size 76.5 MiB',
    },
  });
  await expect(
    waitUntilFDAStatus({ ...options, progress: 80, httpReq }),
  ).rejects.toThrow(
    'Out of Memory Error: could not allocate block of size 76.5 MiB',
  );
  expect(httpReq).toHaveBeenCalledTimes(1);
});

test('performance polling uses the requested visibility and service path', async () => {
  const state = { status: 'completed', progress: 100 };
  const httpReq = jest.fn().mockResolvedValue({ status: 200, json: state });
  await expect(waitUntilFDAStatus({ ...options, httpReq })).resolves.toEqual(
    state,
  );
  expect(httpReq).toHaveBeenCalledWith({
    method: 'GET',
    url: 'http://localhost:1234/private/fdas/traffic-window',
    headers: {
      'Fiware-Service': 'traffic',
      'Fiware-ServicePath': '/fdas',
    },
  });
});

test('performance polling preserves the default public service path', async () => {
  const httpReq = jest.fn().mockResolvedValue({
    status: 200,
    json: { status: 'completed' },
  });
  await waitUntilFDAStatus({
    baseUrl: options.baseUrl,
    service: options.service,
    fdaId: options.fdaId,
    status: 'completed',
    httpReq,
  });
  expect(httpReq.mock.calls[0][0]).toMatchObject({
    url: 'http://localhost:1234/public/fdas/traffic-window',
    headers: { 'Fiware-ServicePath': '/public' },
  });
});
