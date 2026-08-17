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

const DEFAULT_LARGE_TABLE_MIN_ROWS = 5_000_000;

function formatNumber(value) {
  if (value === undefined || value === null) {
    return 'unknown';
  }

  return Number(value).toLocaleString();
}

function formatValue(value) {
  if (value === undefined || value === null) {
    return 'unknown';
  }

  return value;
}

function getMinimumRows() {
  return Number(
    process.env.PERFORMANCE_LARGE_TABLE_MIN_ROWS ??
      DEFAULT_LARGE_TABLE_MIN_ROWS,
  );
}

function validateDatasetInfo(datasetInfo) {
  const minimumRows = getMinimumRows();

  if (!datasetInfo) {
    throw new Error(
      [
        'Large FDA dataset info is missing.',
        'Make sure public.air_quality_test is created in the parent beforeAll',
        'and pass getDatasetInfo: () => largeTableInfo to registerLargeFdaPerformanceTests.',
      ].join(' '),
    );
  }

  if (!datasetInfo.query) {
    throw new Error('Large FDA dataset query is missing.');
  }

  if (!datasetInfo.rows || Number(datasetInfo.rows) < minimumRows) {
    throw new Error(
      [
        `Large FDA dataset is too small.`,
        `Current rows: ${formatNumber(datasetInfo.rows)}.`,
        `Minimum expected rows: ${formatNumber(minimumRows)}.`,
        'Create or recreate public.air_quality_test before running this test.',
      ].join(' '),
    );
  }
}

function logDatasetInfo({ fdaId, datasetInfo }) {
  console.log('\n[PERF] Starting large FDA creation benchmark');

  console.table({
    FDA: fdaId,
    Table: formatValue(datasetInfo.tableName),
    Query: formatValue(datasetInfo.query),
    Rows: formatNumber(datasetInfo.rows),
    Columns: formatValue(datasetInfo.columns),
    'Source table size': formatValue(datasetInfo.size),
    'Source table bytes': formatValue(datasetInfo.bytes),
    'Min timeinstant': formatValue(datasetInfo.minTimeinstant),
    'Max timeinstant': formatValue(datasetInfo.maxTimeinstant),
    Partition: formatValue(datasetInfo.partition),
    Compression: String(datasetInfo.compression),
    Cached: String(datasetInfo.cached),
  });
}

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

async function ensureLargeAirQualityTable(pgClient) {
  const minimumRows = Number(
    process.env.PERFORMANCE_LARGE_TABLE_MIN_ROWS ?? 5_000_000,
  );

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS public.air_quality_test (
      timeinstant timestamptz,
      location text,
      address text,
      dataprovider text,
      name text,
      no2 double precision,
      o3 double precision,
      so2 double precision,
      co double precision,
      co2 double precision,
      pm10 double precision,
      pm25 double precision,
      month int,
      year int
    );
  `);

  const currentRowsResult = await pgClient.query(`
    SELECT COUNT(*)::bigint AS total_rows
    FROM public.air_quality_test;
  `);

  const currentRows = Number(currentRowsResult.rows[0].total_rows);

  if (currentRows < minimumRows) {
    console.log(
      `[TEST] air_quality_test has ${currentRows.toLocaleString()} rows. Recreating large dataset...`,
    );

    await pgClient.query(`
      DROP TABLE IF EXISTS public.air_quality_test;
    `);

    await pgClient.query(`
      CREATE TABLE public.air_quality_test (
        timeinstant timestamptz,
        location text,
        address text,
        dataprovider text,
        name text,
        no2 double precision,
        o3 double precision,
        so2 double precision,
        co double precision,
        co2 double precision,
        pm10 double precision,
        pm25 double precision,
        month int,
        year int
      );
    `);

    await pgClient.query(`
      INSERT INTO public.air_quality_test
      SELECT
        gs AS timeinstant,
        'loc-' || (i % 10) AS location,
        'addr' AS address,
        'prov' AS dataprovider,
        'name' AS name,
        random() * 100 AS no2,
        random() * 100 AS o3,
        random() * 50 AS so2,
        random() * 200 AS co,
        random() * 1000 AS co2,
        random() * 200 AS pm10,
        random() * 100 AS pm25,
        EXTRACT(MONTH FROM gs)::int AS month,
        EXTRACT(YEAR FROM gs)::int AS year
      FROM (
        SELECT generate_series(
          '2024-01-01'::timestamptz,
          '2024-06-30'::timestamptz,
          '5 minutes'
        ) AS gs
      ) t
      CROSS JOIN generate_series(1, 100) AS s(i);
    `);
  } else {
    console.log(
      `[TEST] Reusing existing air_quality_test with ${currentRows.toLocaleString()} rows`,
    );
  }

  const infoResult = await pgClient.query(`
    SELECT
      COUNT(*)::bigint AS rows,
      pg_total_relation_size('public.air_quality_test')::bigint AS bytes,
      pg_size_pretty(
        pg_total_relation_size('public.air_quality_test')
      ) AS size
    FROM public.air_quality_test;
  `);

  const columnsResult = await pgClient.query(`
    SELECT COUNT(*)::int AS columns
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'air_quality_test';
  `);

  const minMaxResult = await pgClient.query(`
    SELECT
      MIN(timeinstant) AS min_timeinstant,
      MAX(timeinstant) AS max_timeinstant
    FROM public.air_quality_test;
  `);

  return {
    tableName: 'public.air_quality_test',
    query: 'SELECT * FROM public.air_quality_test',
    rows: Number(infoResult.rows[0].rows),
    columns: Number(columnsResult.rows[0].columns),
    bytes: Number(infoResult.rows[0].bytes),
    size: infoResult.rows[0].size,
    minTimeinstant: minMaxResult.rows[0],
  };
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
      let fdaId;
      try {
        const baseUrl = getBaseUrl();
        fdaId = `fda-air-quality-large-${Date.now()}`;
        const datasetInfo = await ensureLargeAirQualityTable(pgClient);
        console.log(
          `[PERF] Source dataset: ${datasetInfo.rows.toLocaleString()} rows ` +
            `(table=${datasetInfo.tableSize}, total=${datasetInfo.totalSize})`,
        );
        validateDatasetInfo(datasetInfo);
        logDatasetInfo({ fdaId, datasetInfo });

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
