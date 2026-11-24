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

    test('a query failing in its first rows returns an error instead of a truncated dump', async () => {
      const failingFdaId = 'fda_sql_dump_failing';
      const createFda = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: failingFdaId,
          description: 'fails at runtime on the second row',
          query: `SELECT id, 1 / (id - 2) AS ratio FROM ${SOURCE_TABLE}`,
          cached: false,
        },
      });
      expect(createFda.status).toBe(202);

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(
          getBaseUrl(),
          servicePath,
          failingFdaId,
          'defaultDataAccess',
        ),
        headers: { 'Fiware-Service': service, Accept: 'application/sql' },
      });

      expect(res.status).toBe(500);
      expect(res.headers['content-type']).toContain('application/json');
      expect(res.text).not.toContain('BEGIN;');
    });

    describe('user-defined types and source session settings', () => {
      const typesSchema = 'sql_dump_types';
      const typedTable = 'public.sql_dump_typed';
      const typedFdaId = 'fda_sql_dump_typed';
      const stylesRole = 'sql_dump_styles';
      const stylesTable = 'public.sql_dump_styles';
      const stylesFdaId = 'fda_sql_dump_styles';

      const dropStylesRole = async () => {
        await pgClient.query(
          'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1',
          [stylesRole],
        );
        await pgClient.query(`DROP ROLE IF EXISTS ${stylesRole}`);
      };

      beforeAll(async () => {
        await pgClient.query(`DROP TABLE IF EXISTS ${typedTable}`);
        await pgClient.query(`DROP SCHEMA IF EXISTS ${typesSchema} CASCADE`);
        await pgClient.query(`
          CREATE SCHEMA ${typesSchema};
          CREATE TYPE ${typesSchema}.mood AS ENUM ('ok', 'sad');
          CREATE DOMAIN ${typesSchema}.positive_int AS integer CHECK (VALUE > 0);
          CREATE TABLE ${typedTable} (
            id integer,
            mood ${typesSchema}.mood,
            amount ${typesSchema}.positive_int,
            moods ${typesSchema}.mood[]
          );
          INSERT INTO ${typedTable} VALUES (1, 'ok', 3, '{ok,sad}'), (2, 'sad', NULL, NULL)`);

        await pgClient.query(`DROP TABLE IF EXISTS ${stylesTable}`);
        await dropStylesRole();
        await pgClient.query(`
          CREATE ROLE ${stylesRole} LOGIN PASSWORD 'styles';
          ALTER ROLE ${stylesRole} SET datestyle = 'SQL, DMY';
          ALTER ROLE ${stylesRole} SET intervalstyle = 'sql_standard';
          ALTER ROLE ${stylesRole} SET extra_float_digits = 0;
          CREATE TABLE ${stylesTable} (
            id integer,
            day date,
            local_at timestamp,
            span interval,
            ratio double precision
          );
          INSERT INTO ${stylesTable} VALUES
            (1, '2026-02-01', '2026-02-01 13:45:00', '-1 days +02:00:00', 0.30000000000000004),
            (2, '2026-12-31', '2026-03-04 23:59:59.5', '1 year 2 mons -3 days 04:05:06', 1 / 3::double precision);
          GRANT SELECT ON ${stylesTable} TO ${stylesRole}`);

        const headers = {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        };
        await httpReq({
          method: 'DELETE',
          url: `${getBaseUrl()}/datasources/${stylesRole}`,
          headers: { 'Fiware-Service': service },
        });
        const createDatasource = await httpReq({
          method: 'POST',
          url: `${getBaseUrl()}/datasources`,
          headers: { 'Fiware-Service': service },
          body: {
            datasourceId: stylesRole,
            type: 'postgres',
            config: {
              username: stylesRole,
              password: 'styles',
              host: getPgHost(),
              port: getPgPort(),
              database: service,
            },
          },
        });
        expect(createDatasource.status).toBe(204);

        for (const body of [
          { id: typedFdaId, query: `SELECT * FROM ${typedTable}` },
          {
            id: stylesFdaId,
            query: `SELECT * FROM ${stylesTable}`,
            datasourceId: stylesRole,
          },
        ]) {
          const createFda = await httpReq({
            method: 'POST',
            url: `${getBaseUrl()}/${visibility}/fdas`,
            headers,
            body: {
              description: 'SQL output test fda',
              cached: false,
              ...body,
            },
          });
          expect(createFda.status).toBe(202);
        }
      });

      afterAll(async () => {
        for (const fdaId of [typedFdaId, stylesFdaId]) {
          await httpReq({
            method: 'DELETE',
            url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
            headers: {
              'Fiware-Service': service,
              'Fiware-ServicePath': servicePath,
            },
          });
        }
        await httpReq({
          method: 'DELETE',
          url: `${getBaseUrl()}/datasources/${stylesRole}`,
          headers: { 'Fiware-Service': service },
        });
        await pgClient.query(`DROP TABLE IF EXISTS ${stylesTable}`);
        await dropStylesRole();
        await pgClient.query(`DROP TABLE IF EXISTS ${typedTable}`);
        await pgClient.query(`DROP SCHEMA IF EXISTS ${typesSchema} CASCADE`);
      });

      test('keeps enums and domains from another schema, constraints included', async () => {
        const res = await httpReqRaw({
          method: 'GET',
          url: buildDaDataUrl(
            getBaseUrl(),
            servicePath,
            typedFdaId,
            'defaultDataAccess',
            { sqlSchema: RESTORE_SCHEMA },
          ),
          headers: { 'Fiware-Service': service, Accept: 'application/sql' },
        });

        expect(res.status).toBe(200);
        expect(res.text).toContain(`"mood" ${typesSchema}.mood,`);
        expect(res.text).toContain(`"amount" ${typesSchema}.positive_int,`);
        expect(res.text).toContain(`"moods" ${typesSchema}.mood[]`);

        await pgClient.query(res.text);

        const restoredTable = `${RESTORE_SCHEMA}.${typedFdaId}`;
        const source = await snapshot(pgClient, typedTable);
        const restored = await snapshot(pgClient, restoredTable);
        expect(restored.types).toEqual(source.types);
        expect(restored.rows).toEqual(source.rows);
        await expect(
          pgClient.query(`INSERT INTO ${restoredTable} (amount) VALUES (-1)`),
        ).rejects.toThrow('violates check constraint');
      });

      test.each(['insert', 'copy'])(
        '%s dump restores the same dates, intervals and floats whatever the source and target styles',
        async (sqlFormat) => {
          const table = `styles_${sqlFormat}`;
          const res = await httpReqRaw({
            method: 'GET',
            url: buildDaDataUrl(
              getBaseUrl(),
              servicePath,
              stylesFdaId,
              'defaultDataAccess',
              { sqlSchema: RESTORE_SCHEMA, sqlTable: table, sqlFormat },
            ),
            headers: { 'Fiware-Service': service, Accept: 'application/sql' },
          });

          expect(res.status).toBe(200);
          expect(res.text).toContain('2026-02-01');
          expect(res.text).toContain('0.30000000000000004');
          expect(res.text).not.toContain('01/02/2026');

          await pgClient.query(
            "SET datestyle = 'SQL, MDY'; SET intervalstyle = 'sql_standard'",
          );
          try {
            await (sqlFormat === 'copy'
              ? restoreCopyDump(pgClient, res.text)
              : pgClient.query(res.text));
          } finally {
            await pgClient.query('RESET datestyle; RESET intervalstyle');
          }

          const source = await snapshot(pgClient, stylesTable);
          const restored = await snapshot(
            pgClient,
            `${RESTORE_SCHEMA}.${table}`,
          );
          expect(restored.types).toEqual(source.types);
          expect(restored.rows).toEqual(source.rows);
        },
      );
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
