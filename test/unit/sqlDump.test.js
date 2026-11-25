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

import { describe, expect, test } from '@jest/globals';
import {
  createSqlDump,
  escapeCopyValue,
  quoteSqlIdentifier,
  quoteSqlLiteral,
  splitSqlDumpQueryParams,
} from '../../src/lib/utils/sqlDump.js';

const columns = [
  { name: 'id', type: 'integer' },
  { name: 'timeinstant', type: 'timestamp without time zone' },
];

describe('sqlDump', () => {
  test('quoteSqlIdentifier wraps names in double quotes and doubles inner quotes', () => {
    expect(quoteSqlIdentifier('users')).toBe('"users"');
    expect(quoteSqlIdentifier('my "odd" col')).toBe('"my ""odd"" col"');
  });

  test('quoteSqlLiteral doubles single quotes, keeps backslashes and writes NULL', () => {
    expect(quoteSqlLiteral("o'brien")).toBe("'o''brien'");
    expect(quoteSqlLiteral(String.raw`\x00ff`)).toBe(String.raw`'\x00ff'`);
    expect(quoteSqlLiteral(null)).toBe('NULL');
  });

  test('escapeCopyValue escapes backslashes, tabs, newlines and carriage returns and writes \\N for NULL', () => {
    expect(escapeCopyValue('a\tb\nc\rd\\e')).toBe(String.raw`a\tb\nc\rd\\e`);
    expect(escapeCopyValue(String.raw`\.`)).toBe(String.raw`\\.`);
    expect(escapeCopyValue('')).toBe('');
    expect(escapeCopyValue(null)).toBe(String.raw`\N`);
  });

  test('splitSqlDumpQueryParams removes the SQL params from the DA params and applies defaults', () => {
    expect(
      splitSqlDumpQueryParams({ minAge: '20', sqlTable: 'backup' }, 'sql'),
    ).toEqual({
      queryParams: { minAge: '20' },
      sqlOptions: {
        table: 'backup',
        schema: undefined,
        hypertableColumn: undefined,
        format: 'insert',
      },
    });

    expect(
      splitSqlDumpQueryParams(
        {
          sqlSchema: 'archive',
          sqlHypertable: 'time index',
          sqlFormat: 'copy',
        },
        'sql',
      ).sqlOptions,
    ).toEqual({
      table: undefined,
      schema: 'archive',
      hypertableColumn: 'time index',
      format: 'copy',
    });
  });

  test('splitSqlDumpQueryParams rejects SQL params for other output types', () => {
    expect(splitSqlDumpQueryParams({ minAge: '20' }, 'csv')).toEqual({
      queryParams: { minAge: '20' },
      sqlOptions: undefined,
    });

    expect(() =>
      splitSqlDumpQueryParams({ sqlTable: 't', sqlFormat: 'copy' }, 'json'),
    ).toThrow(
      expect.objectContaining({
        status: 400,
        type: 'BadRequest',
        message:
          'Query params sqlTable, sqlFormat are only allowed with SQL output.',
      }),
    );
  });

  test.each([
    [{ sqlTable: 'bad name' }, 'Query param "sqlTable" must have 1 to 63'],
    [
      { sqlSchema: 'a'.repeat(64) },
      'Query param "sqlSchema" must have 1 to 63',
    ],
    [
      { sqlTable: ['a', 'b'] },
      'Query param "sqlTable" must be a non-empty string.',
    ],
    [
      { sqlHypertable: '' },
      'Query param "sqlHypertable" must be a non-empty string.',
    ],
    [
      { sqlFormat: 'csv' },
      "Invalid sqlFormat 'csv'. Allowed values: insert, copy",
    ],
  ])(
    'splitSqlDumpQueryParams rejects invalid SQL params %j',
    (query, message) => {
      expect(() => splitSqlDumpQueryParams(query, 'sql')).toThrow(
        expect.objectContaining({
          status: 400,
          type: 'BadRequest',
          message: expect.stringContaining(message),
        }),
      );
    },
  );

  test('createSqlDump in insert mode writes one transaction with CREATE TABLE and one INSERT per chunk', () => {
    const dump = createSqlDump({
      columns,
      table: 'fda1_da1',
      format: 'insert',
    });

    const script =
      dump.preamble +
      dump.rows([
        ['1', '2020-08-17 18:25:28.332'],
        ['2', null],
      ]) +
      dump.rows([['3', "it's"]]) +
      dump.epilogue;

    expect(script).toBe(
      [
        '-- FIWARE Data Access SQL dump',
        '',
        'SET statement_timeout = 0;',
        "SET client_encoding = 'UTF8';",
        'SET standard_conforming_strings = on;',
        '',
        'BEGIN;',
        '',
        'CREATE TABLE "fda1_da1" (',
        '    "id" integer,',
        '    "timeinstant" timestamp without time zone',
        ');',
        '',
        'INSERT INTO "fda1_da1" ("id", "timeinstant") VALUES',
        "('1', '2020-08-17 18:25:28.332'),",
        "('2', NULL);",
        'INSERT INTO "fda1_da1" ("id", "timeinstant") VALUES',
        "('3', 'it''s');",
        '',
        'COMMIT;',
        '',
      ].join('\n'),
    );
  });

  test('createSqlDump in copy mode writes a COPY FROM stdin block closed by \\.', () => {
    const dump = createSqlDump({
      columns,
      table: 'backup',
      schema: 'archive',
      hypertableColumn: 'timeinstant',
      format: 'copy',
    });

    const script =
      dump.preamble +
      dump.rows([
        ['1', 'a\tb'],
        ['2', null],
      ]) +
      dump.epilogue;

    expect(script).toContain(
      [
        'BEGIN;',
        '',
        'CREATE SCHEMA IF NOT EXISTS "archive";',
        '',
        'CREATE TABLE "archive"."backup" (',
        '    "id" integer,',
        '    "timeinstant" timestamp without time zone',
        ');',
        '',
        `SELECT create_hypertable('"archive"."backup"', by_range('timeinstant'));`,
        '',
        'COPY "archive"."backup" ("id", "timeinstant") FROM stdin;',
        String.raw`1	a\tb`,
        String.raw`2	\N`,
        String.raw`\.`,
        '',
        'COMMIT;',
        '',
      ].join('\n'),
    );
  });

  test('createSqlDump only creates a hypertable when it is requested', () => {
    expect(
      createSqlDump({ columns, table: 't', format: 'copy' }).preamble,
    ).not.toContain('create_hypertable');
  });

  test('createSqlDump rejects queries with duplicated column names', () => {
    expect(() =>
      createSqlDump({
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'id', type: 'bigint' },
        ],
        table: 't',
      }),
    ).toThrow(
      expect.objectContaining({
        status: 400,
        type: 'BadRequest',
        message: 'SQL output needs unique column names. Duplicated: id.',
      }),
    );
  });

  test('createSqlDump rejects a hypertable column that the query does not return', () => {
    expect(() =>
      createSqlDump({ columns, table: 't', hypertableColumn: 'TimeInstant' }),
    ).toThrow(
      expect.objectContaining({
        status: 400,
        type: 'BadRequest',
        message:
          'sqlHypertable column "TimeInstant" is not returned by the query.',
      }),
    );
  });
});
