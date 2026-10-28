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

import { afterAll, beforeAll, describe, expect, test } from '@jest/globals';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { buildParquetCopyQuery } from '../../src/lib/utils/parquetExport.js';

const ROWS = 12000;
const DAYS = 24;

describe('partitioned Parquet export with real DuckDB', () => {
  let conn;
  let tmpDir;

  beforeAll(async () => {
    const { DuckDBInstance } = await import('@duckdb/node-api');
    const instance = await DuckDBInstance.create(':memory:');
    conn = await instance.connect();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fda-parquet-export-'));

    await conn.run(`
      SET threads = 1;
      SET preserve_insertion_order = false;
      SET partitioned_write_max_open_files = 4;
      SET partitioned_write_flush_threshold = 1000;
    `);
    await conn.run(`
      CREATE TABLE events AS
      SELECT i AS id,
        TIMESTAMP '2025-01-01' + to_seconds((i * ${DAYS} * 86400) // ${ROWS}) AS observed_at,
        md5(i::VARCHAR) AS payload
      FROM range(${ROWS}) t(i)
      ORDER BY hash(i)
    `);
  });

  afterAll(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  async function queryOne(sql) {
    const reader = await conn.runAndReadAll(sql);
    return reader.getRowObjects()[0];
  }

  async function exportEvents(name, sortPartitions) {
    const target = path.join(tmpDir, `${name}.parquet`);
    await conn.run(
      buildParquetCopyQuery(
        'SELECT * FROM events',
        target,
        'observed_at',
        'day',
        false,
        sortPartitions,
      ),
    );

    const layout = await queryOne(`
      SELECT count(*) AS files,
        count(DISTINCT regexp_replace(file, '/[^/]+$', '')) AS partitions
      FROM glob('${target}/**/*.parquet') g(file)
    `);
    const content = await queryOne(`
      SELECT count(*) AS rows,
        count(DISTINCT id) AS ids,
        sum(hash(id, observed_at, payload)) AS checksum,
        count(*) FILTER (
          WHERE year <> year(observed_at)
            OR month <> month(observed_at)
            OR day <> day(observed_at)
        ) AS misplaced
      FROM read_parquet('${target}/**/*.parquet', hive_partitioning = true)
    `);

    return {
      files: Number(layout.files),
      partitions: Number(layout.partitions),
      content,
    };
  }

  test('sorting unordered rows writes far fewer files with the same content', async () => {
    const source = await queryOne(`
      SELECT count(*) AS rows,
        count(DISTINCT id) AS ids,
        sum(hash(id, observed_at, payload)) AS checksum
      FROM events
    `);

    const unsorted = await exportEvents('unsorted', false);
    const sorted = await exportEvents('sorted', true);

    for (const result of [unsorted, sorted]) {
      expect(result.partitions).toBe(DAYS);
      expect(result.content).toEqual({ ...source, misplaced: 0n });
    }
    expect(unsorted.files).toBeGreaterThan(DAYS * 3);
    expect(sorted.files).toBeGreaterThanOrEqual(DAYS);
    expect(sorted.files * 3).toBeLessThan(unsorted.files);
  });
});
