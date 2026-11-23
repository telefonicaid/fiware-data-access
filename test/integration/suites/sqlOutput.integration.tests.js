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
import { Client } from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { GenericContainer, Wait } from 'testcontainers';
import { connectWithRetry } from '../utils/integrationTestUtils.js';

const SOURCE_TABLE = 'public.sql_dump_source';
const RESTORE_SCHEMA = 'sql_dump_restore';
const TIMESCALE_IMAGE = 'timescale/timescaledb:2.28.0-pg16';

const ROW_SNAPSHOT_QUERY = (table) =>
  `SELECT row_to_json(t)::text AS row FROM ${table} t ORDER BY id`;
const COLUMN_TYPES_QUERY = `
  SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
  FROM pg_attribute a
  WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
  ORDER BY a.attnum`;

function pgClientFor(host, port, database) {
  return new Client({
    host,
    port,
    user: 'postgres',
    password: 'postgres',
    database,
    connectionTimeoutMillis: 10_000,
  });
}

async function restoreCopyDump(client, script) {
  const copyLine = /^COPY .+ FROM stdin;$/m.exec(script);
  const dataStart = copyLine.index + copyLine[0].length + 1;
  const terminator = /^\\\.$/m.exec(script.slice(dataStart));
  const data = script.slice(dataStart, dataStart + terminator.index);
  const afterData = script.slice(dataStart + terminator.index + 3);

  await client.query(script.slice(0, copyLine.index));
  await pipeline(
    Readable.from([data]),
    client.query(copyFrom(copyLine[0].slice(0, -1))),
  );
  await client.query(afterData);
}

async function snapshot(client, table) {
  const rows = await client.query(ROW_SNAPSHOT_QUERY(table));
  const types = await client.query(COLUMN_TYPES_QUERY, [table]);
  return {
    rows: rows.rows.map(({ row }) => row),
    types: types.rows,
  };
}

export function registerSqlOutputIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqRaw,
  buildDaDataUrl,
  getPgHost,
  getPgPort,
}) {
  describe('SQL output for only-fresh FDAs', () => {
    const freshFdaId = 'fda_sql_dump';
    const daId = 'da_sql_dump_from_id';
    let pgClient;

    beforeAll(async () => {
      pgClient = pgClientFor(getPgHost(), getPgPort(), service);
      await connectWithRetry(pgClient);

      await pgClient.query(`DROP TABLE IF EXISTS ${SOURCE_TABLE}`);
      await pgClient.query(`DROP SCHEMA IF EXISTS ${RESTORE_SCHEMA} CASCADE`);
      await pgClient.query(`
        CREATE TABLE ${SOURCE_TABLE} (
          id integer,
          label varchar(50),
          price numeric(10,2),
          measured_at timestamptz,
          local_at timestamp(3),
          attrs jsonb,
          tags text[],
          payload bytea,
          active boolean,
          big bigint
        )`);
      await pgClient.query(`
        INSERT INTO ${SOURCE_TABLE} VALUES
          (1, 'plain', 10.50, '2026-11-19T10:00:00Z', '2026-11-19 10:00:00.123',
           '{"unit": "C"}', '{a,b}', '\\x00ff', true, 9007199254740993),
          (2, E'tab\\there\\nnew line \\\\ back o''quote', NULL, '2026-11-19T10:05:00Z', NULL,
           '{"nested": {"q": "it''s"}}', '{"with space","with,comma"}', NULL, false, NULL),
          (3, 'ünïcødé ✓', 0.01, '2026-11-19T11:00:00Z', '2026-11-19 11:00:00',
           NULL, '{}', '\\x', NULL, -1),
          (4, E'\\\\.', 3.00, '2026-11-19T12:00:00Z', '2026-11-19 12:00:00',
           '[]', NULL, '\\x0a09', true, 0)`);

      const createFda = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: freshFdaId,
          description: 'SQL output test fda',
          query: `SELECT * FROM ${SOURCE_TABLE}`,
          cached: false,
        },
      });
      expect(createFda.status).toBe(202);

      const createDa = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/${visibility}/fdas/${freshFdaId}/das`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: daId,
          description: 'rows from an id on',
          query: 'SELECT * WHERE id >= $minId ORDER BY id',
          params: [{ name: 'minId', type: 'Number', required: true }],
        },
      });
      expect(createDa.status).toBe(204);
    });

    afterAll(async () => {
      await pgClient.query(`DROP SCHEMA IF EXISTS ${RESTORE_SCHEMA} CASCADE`);
      await pgClient.query(`DROP TABLE IF EXISTS ${SOURCE_TABLE}`);
      await pgClient.end();
    });

    test('INSERT dump of a DA restores the same rows and column types and refuses to restore twice', async () => {
      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(getBaseUrl(), servicePath, freshFdaId, daId, {
          service,
          servicePath,
          outputType: 'sql',
          minId: 2,
          sqlSchema: RESTORE_SCHEMA,
          sqlTable: 'from_insert',
        }),
      });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toBe(
        'application/sql; charset=utf-8',
      );
      expect(res.headers['content-disposition']).toBe(
        'attachment; filename="results.sql"',
      );
      expect(res.text).not.toContain('COPY ');

      await pgClient.query(res.text);

      const restoredTable = `${RESTORE_SCHEMA}.from_insert`;
      const source = await snapshot(pgClient, SOURCE_TABLE);
      const restored = await snapshot(pgClient, restoredTable);
      expect(restored.types).toEqual(source.types);
      expect(restored.rows).toEqual(source.rows.slice(1));

      await expect(pgClient.query(res.text)).rejects.toThrow(
        'relation "from_insert" already exists',
      );
      await pgClient.query('ROLLBACK');
      expect((await snapshot(pgClient, restoredTable)).rows).toHaveLength(3);
    });

    test('COPY dump of the defaultDataAccess restores every row with psql COPY semantics', async () => {
      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          freshFdaId,
          'defaultDataAccess',
          { sqlSchema: RESTORE_SCHEMA, sqlFormat: 'copy' },
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });

      expect(res.status).toBe(200);
      expect(res.text).toContain(
        `COPY "${RESTORE_SCHEMA}"."${freshFdaId}" ("id", "label", "price"`,
      );

      await restoreCopyDump(pgClient, res.text);

      const source = await snapshot(pgClient, SOURCE_TABLE);
      const restored = await snapshot(
        pgClient,
        `${RESTORE_SCHEMA}.${freshFdaId}`,
      );
      expect(restored.types).toEqual(source.types);
      expect(restored.rows).toEqual(source.rows);
    });

    test('a hypertable dump does not leave anything behind when restored on PostgreSQL without TimescaleDB', async () => {
      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          freshFdaId,
          'defaultDataAccess',
          {
            sqlSchema: RESTORE_SCHEMA,
            sqlTable: 'needs_timescale',
            sqlHypertable: 'measured_at',
          },
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });

      expect(res.status).toBe(200);
      await expect(pgClient.query(res.text)).rejects.toThrow(
        /(create_hypertable|by_range).* does not exist/,
      );
      await pgClient.query('ROLLBACK');

      const table = await pgClient.query(
        `SELECT to_regclass('${RESTORE_SCHEMA}.needs_timescale') AS oid`,
      );
      expect(table.rows[0].oid).toBeNull();
    });

    test('rejects SQL output for cached FDAs, unknown hypertable columns and invalid table names', async () => {
      const cachedFdaId = 'fda_sql_dump_cached';
      const createCachedFda = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: cachedFdaId,
          description: 'cached FDA rejected for SQL output',
          query: `SELECT id FROM ${SOURCE_TABLE}`,
        },
      });
      expect(createCachedFda.status).toBe(202);

      const cached = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          cachedFdaId,
          'defaultDataAccess',
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });
      expect(cached.status).toBe(400);
      expect(cached.json.error).toBe('UnsupportedOutputType');

      const missingColumn = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          freshFdaId,
          'defaultDataAccess',
          { sqlHypertable: 'timeinstant' },
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });
      expect(missingColumn.status).toBe(400);
      expect(missingColumn.json).toEqual({
        error: 'BadRequest',
        description:
          'sqlHypertable column "timeinstant" is not returned by the query.',
      });

      const badTable = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          freshFdaId,
          'defaultDataAccess',
          { sqlTable: 'users; DROP TABLE users' },
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });
      expect(badTable.status).toBe(400);
      expect(badTable.json.error).toBe('BadRequest');
    });

    describe('restored on TimescaleDB', () => {
      let timescale;
      let timescaleClient;

      beforeAll(async () => {
        timescale = await new GenericContainer(TIMESCALE_IMAGE)
          .withEnvironment({ POSTGRES_PASSWORD: 'postgres' })
          .withExposedPorts(5432)
          .withWaitStrategy(
            Wait.forLogMessage(
              /database system is ready to accept connections/,
              2,
            ),
          )
          .start();

        timescaleClient = pgClientFor(
          timescale.getHost(),
          timescale.getMappedPort(5432),
          'postgres',
        );
        await connectWithRetry(timescaleClient);
        await timescaleClient.query(
          'CREATE EXTENSION IF NOT EXISTS timescaledb',
        );
      });

      afterAll(async () => {
        await timescaleClient?.end();
        await timescale?.stop();
      });

      test.each(['insert', 'copy'])(
        '%s dump with sqlHypertable creates a hypertable holding every row',
        async (sqlFormat) => {
          const table = `ts_${sqlFormat}`;
          const res = await httpReqRaw({
            method: 'GET',
            url: buildDaDataUrl(
              getBaseUrl(),
              servicePath,
              freshFdaId,
              'defaultDataAccess',
              {
                sqlSchema: RESTORE_SCHEMA,
                sqlTable: table,
                sqlHypertable: 'measured_at',
                sqlFormat,
              },
            ),
            headers: { 'Fiware-Service': service, Accept: 'application/sql' },
          });
          expect(res.status).toBe(200);

          await (sqlFormat === 'copy'
            ? restoreCopyDump(timescaleClient, res.text)
            : timescaleClient.query(res.text));

          const hypertable = await timescaleClient.query(
            `SELECT d.column_name
             FROM timescaledb_information.dimensions d
             WHERE d.hypertable_schema = $1 AND d.hypertable_name = $2`,
            [RESTORE_SCHEMA, table],
          );
          expect(hypertable.rows).toEqual([{ column_name: 'measured_at' }]);

          const source = await snapshot(pgClient, SOURCE_TABLE);
          const restored = await snapshot(
            timescaleClient,
            `${RESTORE_SCHEMA}.${table}`,
          );
          expect(restored.types).toEqual(source.types);
          expect(restored.rows).toEqual(source.rows);
        },
      );
    });
  });
}
