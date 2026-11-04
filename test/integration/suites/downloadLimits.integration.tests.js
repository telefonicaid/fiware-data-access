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

import { describe, test, expect, beforeAll } from '@jest/globals';
import { ensureDefaultDatasource } from '../utils/integrationTestUtils.js';

export function registerDownloadLimitsIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpFormReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
  getPgHost,
  getPgPort,
}) {
  const headers = {
    'Fiware-Service': service,
    'Fiware-ServicePath': servicePath,
  };

  async function getFda(baseUrl, fdaId) {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers,
    });
    expect(res.status).toBe(200);
    return res.json;
  }

  describe('FDA download limits', () => {
    beforeAll(async () => {
      await ensureDefaultDatasource({
        httpReq,
        baseUrl: getBaseUrl(),
        service,
        getPgHost,
        getPgPort,
      });
    });

    test('cached FDA rejects downloads with 410 once the quota is exhausted', async () => {
      const baseUrl = getBaseUrl();
      const limitedFdaId = 'fda_download_limit';
      const limitedDaId = 'da_download_limit';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers,
        body: {
          id: limitedFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          maxDownloads: 3,
          validityPeriod: '1 day',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: limitedFdaId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${limitedFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: limitedDaId,
          query: 'SELECT id, name ORDER BY id',
        },
      });

      expect(createDa.status).toBe(204);

      const created = await getFda(baseUrl, limitedFdaId);
      expect(created).toMatchObject({
        maxDownloads: 3,
        downloadsUsed: 0,
        remainingDownloads: 3,
        downloadStatus: 'available',
      });
      expect(Date.parse(created.expirationDate)).toBeGreaterThan(Date.now());

      const dataUrl = buildDaDataUrl(
        baseUrl,
        servicePath,
        limitedFdaId,
        limitedDaId,
      );

      for (let i = 0; i < 2; i++) {
        const res = await httpReq({
          method: 'GET',
          url: dataUrl,
          headers: { 'Fiware-Service': service },
        });
        expect(res.status).toBe(200);
        expect(res.json.map((row) => row.name)).toEqual([
          'ana',
          'bob',
          'carlos',
        ]);
      }

      const cdaRes = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${limitedFdaId}`,
          dataAccessId: limitedDaId,
          outputType: 'json',
        },
      });
      expect(cdaRes.status).toBe(200);

      const exhaustedRes = await httpReq({
        method: 'GET',
        url: dataUrl,
        headers: { 'Fiware-Service': service },
      });
      expect(exhaustedRes.status).toBe(410);
      expect(exhaustedRes.json.error).toBe('DownloadLimitReached');

      const exhaustedCdaRes = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${limitedFdaId}`,
          dataAccessId: limitedDaId,
          outputType: 'json',
        },
      });
      expect(exhaustedCdaRes.status).toBe(410);
      expect(exhaustedCdaRes.json.error).toBe('DownloadLimitReached');

      const exhausted = await getFda(baseUrl, limitedFdaId);
      expect(exhausted).toMatchObject({
        maxDownloads: 3,
        downloadsUsed: 3,
        remainingDownloads: 0,
        downloadStatus: 'exhausted',
      });
    });

    test('only-fresh FDA rejects downloads with 410 once the quota is exhausted', async () => {
      const baseUrl = getBaseUrl();
      const freshFdaId = 'fda_download_limit_fresh';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers,
        body: {
          id: freshFdaId,
          query: 'SELECT id, name FROM public.users ORDER BY id',
          cached: false,
          maxDownloads: 1,
        },
      });

      expect(createFda.status).toBe(202);

      const dataUrl = buildDaDataUrl(
        baseUrl,
        servicePath,
        freshFdaId,
        'defaultDataAccess',
      );

      const firstRes = await httpReq({
        method: 'GET',
        url: dataUrl,
        headers: { 'Fiware-Service': service },
      });
      expect(firstRes.status).toBe(200);

      const exhaustedRes = await httpReq({
        method: 'GET',
        url: dataUrl,
        headers: { 'Fiware-Service': service },
      });
      expect(exhaustedRes.status).toBe(410);
      expect(exhaustedRes.json.error).toBe('DownloadLimitReached');

      const exhausted = await getFda(baseUrl, freshFdaId);
      expect(exhausted).toMatchObject({
        maxDownloads: 1,
        downloadsUsed: 1,
        remainingDownloads: 0,
        expirationDate: null,
        downloadStatus: 'exhausted',
      });
    });
  });
}
