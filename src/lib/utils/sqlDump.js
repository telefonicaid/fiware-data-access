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

import { FDAError } from '../fdaError.js';

export const SQL_DUMP_FORMATS = ['insert', 'copy'];
export const SQL_DUMP_QUERY_PARAMS = [
  'sqlTable',
  'sqlSchema',
  'sqlHypertable',
  'sqlFormat',
];

const DEFAULT_SQL_DUMP_FORMAT = 'insert';
const SQL_IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]{1,63}$/;
const COPY_ESCAPES = {
  '\\': '\\\\',
  '\t': String.raw`\t`,
  '\n': String.raw`\n`,
  '\r': String.raw`\r`,
};

export function unsupportedSqlOutputError() {
  return new FDAError(
    400,
    'UnsupportedOutputType',
    'SQL output is only available for only-fresh PostgreSQL FDAs.',
  );
}

export function quoteSqlIdentifier(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

export function quoteSqlLiteral(value) {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  return `'${String(value).replaceAll("'", "''")}'`;
}

export function escapeCopyValue(value) {
  if (value === null || value === undefined) {
    return String.raw`\N`;
  }

  return String(value).replaceAll(/[\\\t\n\r]/g, (char) => COPY_ESCAPES[char]);
}

function parseStringParam(value, name) {
  if (value === undefined) {
    return undefined;
  }

  if (typeof value !== 'string' || value.length === 0) {
    throw new FDAError(
      400,
      'BadRequest',
      `Query param "${name}" must be a non-empty string.`,
    );
  }

  return value;
}

function parseSqlIdentifierParam(value, name) {
  const identifier = parseStringParam(value, name);

  if (identifier !== undefined && !SQL_IDENTIFIER_PATTERN.test(identifier)) {
    throw new FDAError(
      400,
      'BadRequest',
      `Query param "${name}" must have 1 to 63 alphanumeric characters, hyphens or underscores.`,
    );
  }

  return identifier;
}

function parseSqlFormatParam(value) {
  const format =
    parseStringParam(value, 'sqlFormat') ?? DEFAULT_SQL_DUMP_FORMAT;

  if (!SQL_DUMP_FORMATS.includes(format)) {
    throw new FDAError(
      400,
      'BadRequest',
      `Invalid sqlFormat '${format}'. Allowed values: ${SQL_DUMP_FORMATS.join(', ')}`,
    );
  }

  return format;
}

export function splitSqlDumpQueryParams(queryParams, outputType) {
  const remaining = { ...queryParams };
  const provided = {};

  for (const name of SQL_DUMP_QUERY_PARAMS) {
    if (remaining[name] !== undefined) {
      provided[name] = remaining[name];
    }
    delete remaining[name];
  }

  if (outputType !== 'sql') {
    const names = Object.keys(provided);
    if (names.length > 0) {
      throw new FDAError(
        400,
        'BadRequest',
        `Query params ${names.join(', ')} are only allowed with SQL output.`,
      );
    }

    return { queryParams: remaining, sqlOptions: undefined };
  }

  return {
    queryParams: remaining,
    sqlOptions: {
      table: parseSqlIdentifierParam(provided.sqlTable, 'sqlTable'),
      schema: parseSqlIdentifierParam(provided.sqlSchema, 'sqlSchema'),
      hypertableColumn: parseStringParam(
        provided.sqlHypertable,
        'sqlHypertable',
      ),
      format: parseSqlFormatParam(provided.sqlFormat),
    },
  };
}

function assertUniqueColumnNames(columns) {
  const seen = new Set();
  const duplicated = new Set();

  for (const { name } of columns) {
    if (seen.has(name)) {
      duplicated.add(name);
    }
    seen.add(name);
  }

  if (duplicated.size > 0) {
    throw new FDAError(
      400,
      'BadRequest',
      `SQL output needs unique column names. Duplicated: ${[...duplicated].join(', ')}.`,
    );
  }
}

function assertHypertableColumn(columns, hypertableColumn) {
  if (
    hypertableColumn !== undefined &&
    !columns.some(({ name }) => name === hypertableColumn)
  ) {
    throw new FDAError(
      400,
      'BadRequest',
      `sqlHypertable column "${hypertableColumn}" is not returned by the query.`,
    );
  }
}

function buildPreamble({
  qualifiedTable,
  schema,
  columns,
  columnList,
  hypertableColumn,
  format,
}) {
  const lines = [
    '-- FIWARE Data Access SQL dump',
    '',
    'SET statement_timeout = 0;',
    "SET client_encoding = 'UTF8';",
    'SET standard_conforming_strings = on;',
    '',
    'BEGIN;',
    '',
  ];

  if (schema !== undefined) {
    lines.push(
      `CREATE SCHEMA IF NOT EXISTS ${quoteSqlIdentifier(schema)};`,
      '',
    );
  }

  lines.push(
    `CREATE TABLE ${qualifiedTable} (`,
    columns
      .map(({ name, type }) => `    ${quoteSqlIdentifier(name)} ${type}`)
      .join(',\n'),
    ');',
    '',
  );

  if (hypertableColumn !== undefined) {
    lines.push(
      `SELECT create_hypertable(${quoteSqlLiteral(qualifiedTable)}, by_range(${quoteSqlLiteral(hypertableColumn)}));`,
      '',
    );
  }

  if (format === 'copy') {
    lines.push(`COPY ${qualifiedTable} (${columnList}) FROM stdin;`);
  }

  return lines.join('\n') + '\n';
}

function buildCopyLines(rows) {
  return rows
    .map((row) => row.map((value) => escapeCopyValue(value)).join('\t') + '\n')
    .join('');
}

function buildInsertStatement(rows, qualifiedTable, columnList) {
  const values = rows
    .map((row) => `(${row.map((value) => quoteSqlLiteral(value)).join(', ')})`)
    .join(',\n');

  return `INSERT INTO ${qualifiedTable} (${columnList}) VALUES\n${values};\n`;
}

export function createSqlDump({
  columns,
  table,
  schema,
  hypertableColumn,
  format = DEFAULT_SQL_DUMP_FORMAT,
}) {
  assertUniqueColumnNames(columns);
  assertHypertableColumn(columns, hypertableColumn);

  const qualifiedTable =
    schema === undefined
      ? quoteSqlIdentifier(table)
      : `${quoteSqlIdentifier(schema)}.${quoteSqlIdentifier(table)}`;
  const columnList = columns
    .map(({ name }) => quoteSqlIdentifier(name))
    .join(', ');

  return {
    preamble: buildPreamble({
      qualifiedTable,
      schema,
      columns,
      columnList,
      hypertableColumn,
      format,
    }),
    rows:
      format === 'copy'
        ? buildCopyLines
        : (rows) => buildInsertStatement(rows, qualifiedTable, columnList),
    epilogue: format === 'copy' ? '\\.\n\nCOMMIT;\n' : '\nCOMMIT;\n',
  };
}
