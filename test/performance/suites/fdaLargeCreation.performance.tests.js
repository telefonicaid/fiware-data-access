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

import { test, expect } from '@jest/globals';
import { performance } from 'node:perf_hooks';
import { waitUntilFDAStatus } from '../utils/performanceTestUtils';
import { connectWithRetry } from '../../integration/utils/integrationTestUtils.js';
import pg from 'pg';
const { Client } = pg;

function buildLargeFdaBody({ fdaId, datasetInfo }) {
  return {
    id: fdaId,
    query: datasetInfo.query,
    description: 'Performance test: large air quality dataset',
    timeColumn: 'timeinstant',
    objStgConf: {
      partition: datasetInfo.partition ?? 'year',
      compression: datasetInfo.compression ?? false,
    },
    datasourceId: datasetInfo.datasourceId ?? 'default',
    cached: datasetInfo.cached ?? false,
  };
}

function getMeasureDuration(name) {
  return performance.getEntriesByName(name)[0]?.duration ?? 0;
}

export function registerLargeFdaPerformanceTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  maxWaitMs,
}) {
  test(
    'Create large air quality FDA',
    async () => {
      console.log(`[TEST] createLarge air quality FDA`);
      let fdaId;
      const baseUrl = getBaseUrl();
      try {
        fdaId = `fda-air-quality-large-${Date.now()}`;
        const datasetInfo = {
          tableName: 'public.air_quality_test',
          query: 'SELECT * FROM public.air_quality_test',
        };
        const res = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: buildLargeFdaBody({ fdaId, datasetInfo }),
        });

        if (res.status >= 400) {
          console.error('POST /fdas failed:', res.status, res.json ?? res.text);
        }

        expect(res.status).toBe(202);

        const markPrefix = `large-fda-${fdaId}`;

        performance.mark(`${markPrefix}-start`);

        await waitUntilFDAStatus({
          baseUrl,
          service,
          fdaId,
          visibility,
          timeout: maxWaitMs(),
          status: 'fetching',
          progress: 20,
          httpReq,
        });

        performance.mark(`${markPrefix}-fetch-start`);

        await waitUntilFDAStatus({
          baseUrl,
          service,
          fdaId,
          visibility,
          timeout: maxWaitMs(),
          status: 'transforming',
          progress: 60,
          httpReq,
        });

        performance.mark(`${markPrefix}-fetch-end`);

        await waitUntilFDAStatus({
          baseUrl,
          service,
          fdaId,
          visibility,
          timeout: maxWaitMs(),
          status: 'uploading',
          progress: 80,
          httpReq,
        });

        performance.mark(`${markPrefix}-upload-start`);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          visibility,
          timeout: maxWaitMs(),
        });

        performance.mark(`${markPrefix}-end`);

        const creationMeasureName = `Large FDA creation - ${fdaId}`;
        const fetchMeasureName = `Large FDA fetch - ${fdaId}`;
        const transformUploadMeasureName = `Large FDA transform/upload - ${fdaId}`;

        performance.measure(
          creationMeasureName,
          `${markPrefix}-start`,
          `${markPrefix}-end`,
        );

        performance.measure(
          fetchMeasureName,
          `${markPrefix}-fetch-start`,
          `${markPrefix}-fetch-end`,
        );

        performance.measure(
          transformUploadMeasureName,
          `${markPrefix}-fetch-end`,
          `${markPrefix}-upload-start`,
        );

        const creationDuration = getMeasureDuration(creationMeasureName);
        const fetchDuration = getMeasureDuration(fetchMeasureName);
        const transformUploadDuration = getMeasureDuration(
          transformUploadMeasureName,
        );

        console.log(
          `[PERF] Large FDA creation took ${creationDuration.toFixed(
            2,
          )}ms for ${formatNumber(datasetInfo.rows)} rows, ${
            datasetInfo.columns
          } columns, source size ${datasetInfo.size} (fetch: ${fetchDuration.toFixed(
            2,
          )}ms) (transform/upload: ${transformUploadDuration.toFixed(2)}ms)`,
        );
      } finally {
        if (fdaId) {
          const deleteRes = await httpReq({
            method: 'DELETE',
            url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
            headers: {
              'Fiware-Service': service,
              'Fiware-ServicePath': servicePath,
            },
          });
          console.log(`[PERF] Cleanup FDA ${fdaId}: ${deleteRes.status}`);
        }
      }
    },
    maxWaitMs(),
  );
}
