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

import xlsx from 'xlsx';
import { parse as csvParse } from 'csv-parse/sync';
import { CronExpressionParser } from 'cron-parser';

import { FDAError } from '../fdaError.js';
import { normalizeScopedServicePath } from './fdaScope.js';
import { getBasicLogger } from './logger.js';
const logger = getBasicLogger();

export const VALID_VISIBILITIES = ['public', 'private'];
export const VALID_VISIBILITIES_SET = new Set(VALID_VISIBILITIES);

let activeFreshQueries = 0;

function toIsoFromMicros(value) {
  const micros = Number(value);
  if (!Number.isFinite(micros)) {
    return undefined;
  }

  return new Date(micros / 1000).toISOString();
}

function toIsoFromTimestampString(value) {
  const match = value.match(
    /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2})(?::?(\d{2}))?$/,
  );
  if (!match) {
    return undefined;
  }

  const [, datePart, timePart, offsetHours, offsetMinutes = '00'] = match;
  const parsed = new Date(
    `${datePart}T${timePart}${offsetHours}:${offsetMinutes}`,
  );
  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }

  return parsed.toISOString();
}

// Normalize runtime values so downstream serializers emit stable output.
export function normalizeForSerialization(obj) {
  if (typeof obj === 'bigint') {
    return Number(obj);
  }
  if (typeof obj === 'string') {
    return toIsoFromTimestampString(obj) ?? obj;
  }
  if (obj instanceof Date) {
    return obj.toISOString();
  }
  if (Array.isArray(obj)) {
    return obj.map(normalizeForSerialization);
  }
  if (obj !== null && typeof obj === 'object') {
    const keys = Object.keys(obj);
    if (keys.length === 1 && keys[0] === 'micros') {
      const isoDate = toIsoFromMicros(obj.micros);
      if (isoDate) {
        return isoDate;
      }
    }

    const converted = {};
    for (const key in obj) {
      converted[key] = normalizeForSerialization(obj[key]);
    }
    return converted;
  }
  return obj;
}

// Validate that the request body only contains allowed fields
export function validateAllowedFieldsBody(body, allowedFields) {
  const safeBody = body ?? {};
  const keys = Object.keys(safeBody);
  const invalid = keys.filter((k) => !allowedFields.includes(k));
  if (invalid.length > 0) {
    const err = new Error(`Invalid fields in request body, check your request`);
    err.status = 400;
    err.type = 'BadRequest';
    throw err;
  }
}

export function validateForbiddenFieldsQuery(query, forbiddenFields) {
  const keys = Object.keys(query);
  const invalid = keys.filter((k) => forbiddenFields.includes(k));
  if (invalid.length > 0) {
    const err = new Error(
      'Invalid fields in request query, check your request',
    );
    err.status = 400;
    err.type = 'BadRequest';
    throw err;
  }
}

export function parseBooleanQueryParam(value, name, defaultValue = false) {
  if (value === undefined) {
    return defaultValue;
  }

  if (value === true || value === false) {
    return value;
  }

  if (typeof value !== 'string') {
    throw new FDAError(
      400,
      'BadRequest',
      `Query param "${name}" must be a boolean.`,
    );
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === 'true' || normalized === '1') {
    return true;
  }

  if (normalized === 'false' || normalized === '0') {
    return false;
  }

  throw new FDAError(
    400,
    'BadRequest',
    `Query param "${name}" must be a boolean.`,
  );
}

export function assertFreshQueriesEnabled(isEnabled) {
  if (!isEnabled) {
    throw new FDAError(
      503,
      'SyncQueriesDisabled',
      'Fresh query mode is disabled in this instance',
    );
  }
}

export function acquireFreshQuerySlot(maxConcurrent) {
  const parsedMax = Number(maxConcurrent);
  const maxFreshQueries = Number.isFinite(parsedMax)
    ? Math.max(1, parsedMax)
    : 5;

  if (activeFreshQueries >= maxFreshQueries) {
    throw new FDAError(
      429,
      'TooManyFreshQueries',
      `Too many concurrent fresh queries (limit ${maxFreshQueries})`,
    );
  }

  activeFreshQueries += 1;

  let released = false;
  return () => {
    if (released) {
      return;
    }

    released = true;
    activeFreshQueries = Math.max(0, activeFreshQueries - 1);
  };
}

export function getWindowDate(windowSize) {
  const now = new Date();

  const map = {
    day: () => now.setDate(now.getDate() - 1),
    week: () => now.setDate(now.getDate() - 7),
    month: () => now.setMonth(now.getMonth() - 1),
    year: () => now.setFullYear(now.getFullYear() - 1),
  };

  map[windowSize]?.();
  return map[windowSize] ? now : undefined;
}

export function convertRefreshIntervalToMs(interval) {
  if (!interval || typeof interval !== 'string' || !interval.trim()) {
    return null;
  }

  const normalized = interval.trim().toLowerCase();

  // Parse human-readable Agenda format: "number unit"
  const match = normalized.match(
    /^(\d+)\s*(second|minute|hour|day|week|month|year)s?$/,
  );
  if (match) {
    const [, quantity, unit] = match;
    const num = parseInt(quantity, 10);
    const unitMs = {
      second: 1000,
      minute: 60 * 1000,
      hour: 60 * 60 * 1000,
      day: 24 * 60 * 60 * 1000,
      week: 7 * 24 * 60 * 60 * 1000,
      month: 30 * 24 * 60 * 60 * 1000,
      year: 365 * 24 * 60 * 60 * 1000,
    };
    return num * (unitMs[unit] || 0);
  }

  // Cron intervals
  const cronMs = cronToIntervalMs(interval);
  if (cronMs !== null) {
    return cronMs;
  }

  return null;
}

function cronToIntervalMs(cron) {
  let interval;

  try {
    interval = CronExpressionParser.parse(cron);
  } catch {
    return null;
  }

  if (!interval) {
    return null;
  }

  // We need to get the difference between two consecutive runs to know the actual interval
  const next = interval.next().getTime();
  const next2 = interval.next().getTime();

  return next2 - next;
}

export function getTimeColumnQuery(query, timeColumn) {
  if (typeof timeColumn !== 'string' || !/^[a-zA-Z0-9_]+$/.test(timeColumn)) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid time column name "${timeColumn}".`,
    );
  }

  const upper = query.toUpperCase();
  const start = upper.indexOf('SELECT');

  if (start === -1) {
    throw new FDAError(
      400,
      'InvalidParam',
      `Invalid query format. Missing SELECT statement.`,
    );
  }

  // Search for the position to insert the time column, which is right after the SELECT keyword and any following whitespace
  let insertPos = start + 6;
  while (query[insertPos] === ' ') {
    insertPos++;
  }

  // find end of SELECT clause (before FROM)
  const fromIndex = upper.indexOf('FROM');
  const selectClause = query.slice(insertPos, fromIndex).trim();
  const columns = selectClause.split(',').map((c) => c.trim());

  // Check if SELECT already has the timecolumn
  if (selectClause.startsWith('*') || columns.includes(timeColumn)) {
    return query;
  }

  return query.slice(0, insertPos) + timeColumn + ', ' + query.slice(insertPos);
}

export function stringifyCsvValue(value) {
  const normalizedValue = normalizeForSerialization(value);

  if (normalizedValue === null || normalizedValue === undefined) {
    return '';
  }

  if (typeof normalizedValue === 'object') {
    return JSON.stringify(normalizedValue);
  }

  return String(normalizedValue);
}

export async function writeCsvLine(res, line) {
  const ok = res.write(line);
  if (!ok) {
    await new Promise((resolve) => res.once('drain', resolve));
  }
}

export async function writeNdjsonLine(res, row) {
  const safeObj = normalizeForSerialization(row);
  const ok = res.write(JSON.stringify(safeObj) + '\n');
  if (!ok) {
    await new Promise((resolve) => res.once('drain', resolve));
  }
}

export async function writeCsvHeader(res, columnNames) {
  if (columnNames.length === 0) {
    return;
  }

  await writeCsvLine(
    res,
    columnNames.map((columnName) => escapeCsvValue(columnName)).join(',') +
      '\n',
  );
}

export function toRowObject(row, columnNames) {
  const rowObj = {};

  for (let i = 0; i < columnNames.length; i++) {
    rowObj[columnNames[i]] = row[i];
  }

  return rowObj;
}

export function escapeCsvValue(value) {
  const strValue = stringifyCsvValue(value);

  if (
    strValue.includes(',') ||
    strValue.includes('"') ||
    strValue.includes('\n') ||
    strValue.includes('\r')
  ) {
    return '"' + strValue.replace(/"/g, '""') + '"';
  }

  return strValue;
}

/**
 * Parses a CSV buffer and returns headers and the original CSV content.
 * Uses csv-parse to robustly extract headers handling quotes and delimiters.
 */
function parseCsvBuffer(buffer) {
  const content = buffer.toString('utf-8');
  // Use sync parser with columns:true to get header names, limit to 1 row
  let records;
  try {
    records = csvParse(content, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true, // allow inconsistent columns (we only need headers)
      to: 1, // only parse first data row
    });
  } catch (err) {
    throw new Error(`Invalid CSV format: ${err.message}`);
  }
  if (records.length === 0) {
    throw new Error('CSV file has no header row');
  }
  const headers = Object.keys(records[0]);
  if (headers.length === 0) {
    throw new Error('CSV header row is empty');
  }
  return { csvContent: content, headers };
}

function ensureBuffer(buffer) {
  if (Buffer.isBuffer(buffer)) {
    return buffer;
  }

  if (buffer instanceof ArrayBuffer || buffer instanceof Uint8Array) {
    return Buffer.from(buffer);
  }

  if (buffer && typeof buffer === 'object' && buffer.buffer) {
    return Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  }

  return Buffer.from(buffer);
}

function readSheetRows(sheet) {
  // Strategy 1: Standard read
  let rows = xlsx.utils.sheet_to_json(sheet, {
    header: 1,
    defval: '',
    blankrows: true,
    raw: true,
  });

  if (rows && rows.length > 0) {
    return rows;
  }

  // Strategy 2: Manual cell reading (fallback for complex sheets)
  const range = sheet['!ref'];
  if (!range) {
    return [];
  }

  const decodedRange = xlsx.utils.decode_range(range);
  const manualRows = [];

  for (let R = decodedRange.s.r; R <= decodedRange.e.r; R++) {
    const row = [];
    let hasContent = false;

    for (let C = decodedRange.s.c; C <= decodedRange.e.c; C++) {
      const cellAddress = xlsx.utils.encode_cell({ r: R, c: C });
      const cell = sheet[cellAddress];
      let value = '';

      if (cell) {
        // Try different value formats
        if (cell.v !== undefined) {
          value = cell.v;
        } else if (cell.w !== undefined) {
          value = cell.w;
        } else {
          value = '';
        }

        if (value !== '' && value !== null && value !== undefined) {
          hasContent = true;
        }
      }

      row.push(value !== undefined && value !== null ? String(value) : '');
    }

    if (hasContent) {
      manualRows.push(row);
    }
  }

  return manualRows;
}

function findHeaderRow(rows) {
  for (let i = 0; i < rows.length; i++) {
    const hasContent = rows[i].some(
      (cell) =>
        cell !== '' &&
        cell !== null &&
        cell !== undefined &&
        String(cell).trim() !== '',
    );
    if (hasContent) {
      return i;
    }
  }
  return -1;
}

function parseXlsxBuffer(buffer) {
  const fileBuffer = ensureBuffer(buffer);

  let workbook;
  try {
    workbook = xlsx.read(fileBuffer, {
      type: 'buffer',
      cellDates: false,
      cellNF: false,
      cellText: false,
      raw: true,
    });
  } catch (err) {
    throw new Error(`Failed to read Excel file: ${err.message}`);
  }

  const sheetNames = workbook.SheetNames;
  if (sheetNames.length === 0) {
    throw new Error('XLSX file contains no sheets');
  }

  const unifiedHeadersSet = new Set();
  const allSheetsData = [];

  for (const sheetName of sheetNames) {
    const sheet = workbook.Sheets[sheetName];

    const rows = readSheetRows(sheet);
    if (!rows || rows.length === 0) {
      continue;
    }

    const headerRowIndex = findHeaderRow(rows);
    if (headerRowIndex === -1) {
      continue;
    }

    const headerRow = rows[headerRowIndex];
    const headers = headerRow.map((h) =>
      h !== '' && h !== null && h !== undefined ? String(h).trim() : '',
    );

    const validHeaders = headers.filter((h) => h !== '');
    if (validHeaders.length === 0) {
      continue;
    }

    const dataRows = rows.slice(headerRowIndex + 1);
    allSheetsData.push({
      headers,
      validHeaders,
      dataRows,
    });

    for (const h of validHeaders) {
      unifiedHeadersSet.add(h);
    }
  }

  if (unifiedHeadersSet.size === 0) {
    throw new Error('No headers found in any sheet');
  }

  const unifiedHeaders = Array.from(unifiedHeadersSet);

  const allRows = [];

  for (const sheetData of allSheetsData) {
    const { headers, dataRows } = sheetData;

    const headerMap = headers.map((h) =>
      h === '' ? -1 : unifiedHeaders.indexOf(h),
    );

    for (const row of dataRows) {
      const hasContent = row.some(
        (cell) =>
          cell !== '' &&
          cell !== null &&
          cell !== undefined &&
          String(cell).trim() !== '',
      );
      if (!hasContent) continue;

      const obj = {};
      let hasValues = false;

      const maxLen = Math.max(row.length, headerMap.length);
      for (let j = 0; j < maxLen; j++) {
        const unifiedIdx = j < headerMap.length ? headerMap[j] : -1;
        if (unifiedIdx !== -1) {
          const colName = unifiedHeaders[unifiedIdx];
          const value =
            j < row.length &&
            row[j] !== '' &&
            row[j] !== undefined &&
            row[j] !== null
              ? row[j]
              : null;
          obj[colName] = value;
          if (value !== null) hasValues = true;
        }
      }

      if (hasValues) {
        allRows.push(obj);
      }
    }
  }

  if (allRows.length === 0) {
    throw new Error('No data rows found in any sheet');
  }

  const csvRows = [];
  csvRows.push(unifiedHeaders.map((h) => escapeCsvValue(h)).join(','));

  for (const rowObj of allRows) {
    const line = unifiedHeaders
      .map((col) => {
        const value = rowObj[col];
        return escapeCsvValue(
          value !== undefined && value !== null ? value : '',
        );
      })
      .join(',');
    csvRows.push(line);
  }

  return { csvContent: csvRows.join('\n'), headers: unifiedHeaders };
}

export function parseUploadedFile(buffer, mimetype, originalname) {
  const fileBuffer = ensureBuffer(buffer);

  // Detect file type by magic bytes
  const isXlsxByMagic =
    fileBuffer.length >= 4 &&
    fileBuffer[0] === 0x50 &&
    fileBuffer[1] === 0x4b &&
    fileBuffer[2] === 0x03 &&
    fileBuffer[3] === 0x04;

  let csvContent;
  let headers;

  const isCsv = mimetype === 'text/csv' || /\.csv$/i.test(originalname);
  const isXls =
    /\.xls$/i.test(originalname) || mimetype === 'application/vnd.ms-excel';
  const isXlsx =
    /\.xlsx$/i.test(originalname) ||
    mimetype ===
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
    isXlsxByMagic;

  if (isCsv) {
    const result = parseCsvBuffer(fileBuffer);
    csvContent = result.csvContent;
    headers = result.headers;
  } else if (isXls || isXlsx) {
    const result = parseXlsxBuffer(fileBuffer);
    csvContent = result.csvContent;
    headers = result.headers;
  } else {
    throw new FDAError(
      415,
      'UnsupportedMediaType',
      'Only CSV, XLS, or XLSX files are allowed',
    );
  }

  return { csvContent, headers };
}

export function normalizeVisibility(visibility) {
  if (!VALID_VISIBILITIES_SET.has(visibility)) {
    throw new FDAError(
      400,
      'InvalidVisibility',
      'Visibility must be public or private',
    );
  }

  return visibility;
}

export function normalizeServicePath(servicePath) {
  try {
    return normalizeScopedServicePath(servicePath);
  } catch (error) {
    if (error.message === 'servicePath is required') {
      throw new FDAError(
        400,
        'InvalidServicePath',
        'Fiware-ServicePath header is required',
      );
    }

    throw new FDAError(
      400,
      'InvalidServicePath',
      'Fiware-ServicePath must be a non-root absolute path (e.g. /servicepath)',
    );
  }
}

export function toFDAApiResponse(fda, { includeId }) {
  if (!fda) {
    return fda;
  }

  const response = { ...fda };
  const fdaId = response.fdaId;

  delete response._id;
  delete response.fdaId;
  delete response.service;
  delete response.visibility;
  delete response.servicePath;

  if (!includeId) {
    return response;
  }

  return {
    id: fdaId,
    ...response,
  };
}
