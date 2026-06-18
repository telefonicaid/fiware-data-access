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
// an express reference to TSOL ownership in the software and / or computer program.
//
// Non-fulfillment of the provisions set forth herein and, in general, any violation of
// the peaceful possession and ownership of these rights will be prosecuted by the means
// provided in both Spanish and international law. TSOL reserves any civil or
// criminal actions it may exercise to protect its rights.

import { test, expect } from '@jest/globals';
import { calculatePercentile } from '../utils/performanceTestUtils';

const DEFAULT_LOAD_FDA_COUNT = 5;

export function registerFdaLoadPerformanceTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqNoBody,
  waitUntilFDACompleted,
  maxWaitMs,
  loadFdaCount,
  loadFdaRampUpMs,
  buildDaDataUrl,
}) {
  describe('FDA load tests', () => {
    const effectiveLoadFdaCount = Number(
      loadFdaCount ?? DEFAULT_LOAD_FDA_COUNT,
    );
    const query =
      'SELECT id, name, age, timeinstant, authorized, country, score FROM public.users ORDER BY id';

    test(
      'Create and complete multiple FDAs concurrently',
      async () => {
        const baseUrl = getBaseUrl();
        const submitState = { firstMarked: false };

        // Phase 1: Submit all FDAs with optional ramp-up
        const submitRequests = [];
        const submitIntervals = loadFdaRampUpMs
          ? loadFdaRampUpMs / effectiveLoadFdaCount
          : 0;

        for (let index = 0; index < effectiveLoadFdaCount; index += 1) {
          const fdaId = `perf-load-${index + 1}`;
          const requestPromise = new Promise((resolve) => {
            const delay = Math.round(index * submitIntervals);
            setTimeout(async () => {
              if (!submitState.firstMarked) {
                performance.mark('ramp-first-submit');
                submitState.firstMarked = true;
              }
              if (index === effectiveLoadFdaCount - 1) {
                performance.mark('ramp-last-submit');
              }
              const res = await httpReq({
                method: 'POST',
                url: `${baseUrl}/${visibility}/fdas`,
                headers: {
                  'Fiware-Service': service,
                  'Fiware-ServicePath': servicePath,
                },
                body: {
                  id: fdaId,
                  query,
                  description: `Performance load test FDA ${index + 1}`,
                },
              });
              resolve({ fdaId, res, submitDelayMs: delay });
            }, delay);
          });
          submitRequests.push(requestPromise);
        }

        const submitResults = await Promise.all(submitRequests);

        // Measure actual ramp-up duration
        if (submitState.firstMarked) {
          performance.measure(
            'Creation ramp up',
            'ramp-first-submit',
            'ramp-last-submit',
          );
        }

        const successfulSubmits = submitResults.filter(
          ({ res }) => res.status === 202,
        );
        const failedSubmits = submitResults.filter(
          ({ res }) => res.status >= 400 || res.status !== 202,
        );

        if (failedSubmits.length > 0) {
          console.error(
            '[PERF] Some create requests failed:',
            failedSubmits.map(({ fdaId, res }) => ({
              fdaId,
              status: res.status,
            })),
          );
        }

        expect(successfulSubmits.length).toBe(effectiveLoadFdaCount);

        // Phase 2: Wait for all FDAs to complete
        performance.mark('load-complete-start');
        const fdaIds = successfulSubmits.map(({ fdaId }) => fdaId);
        const completionTimes = [];

        const waitRequests = fdaIds.map((fdaId) =>
          (async () => {
            const startMs = Date.now();
            await waitUntilFDACompleted({
              baseUrl,
              service,
              fdaId,
              visibility,
              timeout: maxWaitMs(),
            });
            const durationMs = Date.now() - startMs;
            completionTimes.push({ fdaId, durationMs });
            return { fdaId, durationMs };
          })(),
        );

        await Promise.all(waitRequests);
        performance.mark('load-complete-end');

        // Calculate timing statistics
        const sortedDurations = completionTimes
          .map(({ durationMs }) => durationMs)
          .sort((a, b) => a - b);

        const p50 = calculatePercentile(sortedDurations, 50);
        const p90 = calculatePercentile(sortedDurations, 90);
        const p95 = calculatePercentile(sortedDurations, 95);
        const p99 = calculatePercentile(sortedDurations, 99);
        const minDuration = sortedDurations[0];
        const maxDuration = sortedDurations[sortedDurations.length - 1];
        const avgDuration =
          sortedDurations.reduce((a, b) => a + b, 0) / sortedDurations.length;

        const submitMeasure = performance.getEntriesByName(
          'Creation load submit',
        )[0];
        const submitDuration = submitMeasure?.duration ?? 0;

        performance.measure(
          'Creation load completion',
          'load-complete-start',
          'load-complete-end',
        );
        const completeMeasure = performance.getEntriesByName(
          'Creation load completion',
        )[0];
        const completeDuration = completeMeasure?.duration ?? 0;

        const rampUpMeasure =
          performance.getEntriesByName('Creation ramp up')[0];
        const rampUpDuration = rampUpMeasure?.duration ?? 0;
        const submitThroughput =
          effectiveLoadFdaCount / (submitDuration / 1000 || 1);
        const completionThroughput =
          effectiveLoadFdaCount / (completeDuration / 1000 || 1);

        // Log results
        console.log(
          `\n[PERF] Concurrent FDA Creation Load Test (${effectiveLoadFdaCount} FDAs):`,
        );
        console.log(
          `  Submission phase: ${submitDuration.toFixed(2)}ms for ${effectiveLoadFdaCount} requests`,
        );
        console.log(
          `  Completion phase: ${completeDuration.toFixed(2)}ms to complete all FDAs`,
        );
        console.log(
          `  Ramp-up interval: ${loadFdaRampUpMs}ms across ${effectiveLoadFdaCount} requests`,
        );
        console.log(
          `  Effective submit throughput: ${submitThroughput.toFixed(2)} req/s`,
        );
        console.log(
          `  Effective completion throughput: ${completionThroughput.toFixed(2)} req/s`,
        );
        console.log('\n  Per-FDA completion times:');
        console.table({
          'Min (ms)': minDuration.toFixed(2),
          'P50 (ms)': p50.toFixed(2),
          'P90 (ms)': p90.toFixed(2),
          'P95 (ms)': p95.toFixed(2),
          'P99 (ms)': p99.toFixed(2),
          'Max (ms)': maxDuration.toFixed(2),
          'Avg (ms)': avgDuration.toFixed(2),
          'Total (ms)': completeDuration.toFixed(2),
          'Ramp-up (ms)': rampUpDuration.toFixed(2),
        });
      },
      maxWaitMs(),
    );

    test(
      'Execute multiple queries concurrently against created FDAs',
      async () => {
        const baseUrl = getBaseUrl();

        // Build list of fdaIds that the creation test used
        const fdaIds = [];
        for (let i = 0; i < effectiveLoadFdaCount; i += 1) {
          fdaIds.push(`perf-load-${i + 1}`);
        }

        // Wait each FDA to be completed before querying
        for (const fdaId of fdaIds) {
          await waitUntilFDACompleted({
            baseUrl,
            service,
            fdaId,
            visibility,
            timeout: maxWaitMs(),
          });
        }

        const submitState = { firstMarked: false };
        performance.mark('query-load-start');

        const submitIntervals = loadFdaRampUpMs
          ? Math.round(loadFdaRampUpMs / effectiveLoadFdaCount)
          : 0;

        const completionTimes = [];
        const requests = fdaIds.map(
          (fdaId, index) =>
            new Promise((resolve) => {
              const delay = Math.round(index * submitIntervals);
              setTimeout(async () => {
                if (!submitState.firstMarked) {
                  performance.mark('ramp-first-query-load');
                  submitState.firstMarked = true;
                }
                if (index === fdaIds.length - 1) {
                  performance.mark('ramp-last-query-load');
                }

                const url = buildDaDataUrl(
                  baseUrl,
                  servicePath,
                  fdaId,
                  'defaultDataAccess',
                  { pageSize: 100, pageStart: 0 },
                );

                const startMs = Date.now();
                const res = await httpReqNoBody({
                  method: 'GET',
                  url,
                  headers: {
                    'Fiware-Service': service,
                    'Fiware-ServicePath': servicePath,
                    Accept: 'application/json',
                  },
                });
                const durationMs = Date.now() - startMs;
                completionTimes.push({ fdaId, durationMs, status: res.status });
                resolve({
                  fdaId,
                  durationMs,
                  status: res.status,
                  submitDelayMs: delay,
                });
              }, delay);
            }),
        );

        await Promise.all(requests);
        performance.mark('query-load-end');

        if (submitState.firstMarked) {
          performance.measure(
            'Query ramp up',
            'ramp-first-query-load',
            'ramp-last-query-load',
          );
        }

        const sortedDurations = completionTimes
          .map(({ durationMs }) => durationMs)
          .sort((a, b) => a - b);

        const p50 = calculatePercentile(sortedDurations, 50);
        const p90 = calculatePercentile(sortedDurations, 90);
        const p95 = calculatePercentile(sortedDurations, 95);
        const p99 = calculatePercentile(sortedDurations, 99);
        const minDuration = sortedDurations[0];
        const maxDuration = sortedDurations[sortedDurations.length - 1];
        const avgDuration =
          sortedDurations.reduce((a, b) => a + b, 0) / sortedDurations.length;

        performance.measure(
          'Query load completion',
          'query-load-start',
          'query-load-end',
        );
        const measure = performance.getEntriesByName(
          'Query load completion',
        )[0];
        const totalDuration = measure?.duration ?? 0;

        const rampMeasure = performance.getEntriesByName('Query ramp up')[0];
        const rampUpDuration = rampMeasure?.duration ?? 0;

        const successful = completionTimes.filter(
          ({ status }) => status === 200,
        );
        const failed = completionTimes.filter(
          ({ status }) => status >= 400 || status !== 200,
        );

        if (failed.length > 0) {
          console.error(
            '[PERF] Some query load requests failed:',
            failed.map(({ fdaId, status }) => ({ fdaId, status })),
          );
        }

        expect(successful.length).toBe(fdaIds.length);

        const throughput = fdaIds.length / (totalDuration / 1000 || 1);

        console.log(
          `\n[PERF] Concurrent Query Load Test (${fdaIds.length} queries):`,
        );
        console.log(
          `  Total time: ${totalDuration.toFixed(2)}ms for ${fdaIds.length} requests`,
        );
        console.log(
          `  Ramp-up interval: ${loadFdaRampUpMs}ms across ${fdaIds.length} requests`,
        );
        console.log(`  Effective throughput: ${throughput.toFixed(2)} req/s`);
        console.log('\n  Per-request times:');
        console.table({
          'Min (ms)': minDuration.toFixed(2),
          'P50 (ms)': p50.toFixed(2),
          'P90 (ms)': p90.toFixed(2),
          'P95 (ms)': p95.toFixed(2),
          'P99 (ms)': p99.toFixed(2),
          'Max (ms)': maxDuration.toFixed(2),
          'Avg (ms)': avgDuration.toFixed(2),
          'Total (ms)': totalDuration.toFixed(2),
          'Ramp-up (ms)': rampUpDuration.toFixed(2),
        });
      },
      maxWaitMs(),
    );
  });
}
