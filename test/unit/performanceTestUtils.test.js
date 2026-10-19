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
