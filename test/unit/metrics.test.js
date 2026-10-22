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

const getDuckDBMemorySnapshotMock = jest.fn();
const getOperationalCollectionsSnapshotMock = jest
  .fn()
  .mockRejectedValue(new Error('mongo unavailable in unit tests'));

await jest.unstable_mockModule('../../src/lib/utils/db.js', () => ({
  getDuckDBMemorySnapshot: getDuckDBMemorySnapshotMock,
}));

await jest.unstable_mockModule('../../src/lib/utils/mongo.js', () => ({
  getOperationalCollectionsSnapshot: getOperationalCollectionsSnapshotMock,
}));

const { buildHealthPayload, buildMetricsText } = await import(
  '../../src/lib/metrics.js'
);

const duckdbSnapshot = {
  limitBytes: 1024,
  usedBytes: 512,
  tempBytes: 64,
  byTag: { COLUMN_DATA: 512 },
};

describe('metrics DuckDB memory', () => {
  beforeEach(() => {
    getDuckDBMemorySnapshotMock.mockReset();
  });

  test('buildMetricsText exposes DuckDB memory gauges', async () => {
    getDuckDBMemorySnapshotMock.mockResolvedValue(duckdbSnapshot);

    const text = await buildMetricsText();

    expect(text).toContain('fda_duckdb_memory_limit_bytes 1024');
    expect(text).toContain('fda_duckdb_memory_used_bytes 512');
    expect(text).toContain('fda_duckdb_temp_storage_bytes 64');
    expect(text).toContain(
      'fda_duckdb_memory_usage_bytes{tag="COLUMN_DATA"} 512',
    );
    expect(text).toMatch(/# EOF\n$/);
  });

  test('buildMetricsText omits DuckDB gauges when DuckDB is not initialised', async () => {
    getDuckDBMemorySnapshotMock.mockResolvedValue(null);

    const text = await buildMetricsText();

    expect(text).not.toContain('fda_duckdb_');
    expect(text).toMatch(/# EOF\n$/);
  });

  test('buildHealthPayload includes a duckdb block when DuckDB is initialised', async () => {
    getDuckDBMemorySnapshotMock.mockResolvedValue(duckdbSnapshot);

    const payload = await buildHealthPayload();

    expect(payload.duckdb).toEqual({
      memoryLimitBytes: 1024,
      memoryUsedBytes: 512,
      tempStorageBytes: 64,
      memoryUsageByTagBytes: { COLUMN_DATA: 512 },
    });
  });

  test('buildHealthPayload omits the duckdb block when DuckDB is not initialised', async () => {
    getDuckDBMemorySnapshotMock.mockResolvedValue(null);

    const payload = await buildHealthPayload();

    expect(payload).not.toHaveProperty('duckdb');
  });
});
