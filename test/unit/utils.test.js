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
import xlsx from 'xlsx';

const loggerMock = {
  debug: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
};

const cronParserMock = {
  parse: jest.fn(),
};

async function loadUtilsModule() {
  jest.resetModules();

  loggerMock.debug.mockClear();
  loggerMock.info.mockClear();
  loggerMock.error.mockClear();
  cronParserMock.parse.mockClear();

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  await jest.unstable_mockModule('cron-parser', () => ({
    CronExpressionParser: cronParserMock,
  }));

  await jest.unstable_mockModule('xlsx', () => ({
    default: xlsx,
    ...xlsx,
  }));

  return import('../../src/lib/utils/utils.js');
}

describe('utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('normalizeForSerialization', () => {
    test('converts Date values to ISO strings recursively', async () => {
      const { normalizeForSerialization } = await loadUtilsModule();
      const value = {
        date: new Date('2026-04-08T10:11:12.000Z'),
        nested: [new Date('2026-04-08T10:11:13.000Z')],
      };

      expect(normalizeForSerialization(value)).toEqual({
        date: '2026-04-08T10:11:12.000Z',
        nested: ['2026-04-08T10:11:13.000Z'],
      });
    });

    test('converts bigint values to numbers recursively', async () => {
      const { normalizeForSerialization } = await loadUtilsModule();

      expect(
        normalizeForSerialization({
          count: 1n,
          nested: { values: [2n] },
        }),
      ).toEqual({
        count: 1,
        nested: { values: [2] },
      });
    });
  });

  describe('parseBooleanQueryParam', () => {
    test('returns false when value is undefined', async () => {
      const { parseBooleanQueryParam } = await loadUtilsModule();

      expect(parseBooleanQueryParam(undefined, 'fresh')).toBe(false);
    });

    test('returns boolean value unchanged for true/false input', async () => {
      const { parseBooleanQueryParam } = await loadUtilsModule();

      expect(parseBooleanQueryParam(true, 'fresh')).toBe(true);
      expect(parseBooleanQueryParam(false, 'fresh')).toBe(false);
    });

    test('parses "true"/"1" as true and "false"/"0" as false', async () => {
      const { parseBooleanQueryParam } = await loadUtilsModule();

      expect(parseBooleanQueryParam('true', 'fresh')).toBe(true);
      expect(parseBooleanQueryParam('1', 'fresh')).toBe(true);
      expect(parseBooleanQueryParam('false', 'fresh')).toBe(false);
      expect(parseBooleanQueryParam('0', 'fresh')).toBe(false);
    });

    test('throws FDAError for invalid string values', async () => {
      const { parseBooleanQueryParam } = await loadUtilsModule();

      expect(() => parseBooleanQueryParam('notabool', 'fresh')).toThrow(
        'Query param "fresh" must be a boolean.',
      );
    });

    test('throws FDAError for non-string non-boolean values', async () => {
      const { parseBooleanQueryParam } = await loadUtilsModule();

      expect(() => parseBooleanQueryParam(123, 'fresh')).toThrow(
        'Query param "fresh" must be a boolean.',
      );
    });
  });

  describe('fresh query slot system', () => {
    test('assertFreshQueriesEnabled throws when passed false', async () => {
      const { assertFreshQueriesEnabled } = await loadUtilsModule();

      expect(() => assertFreshQueriesEnabled(false)).toThrow(
        'Fresh query mode is disabled in this instance',
      );
    });

    test('assertFreshQueriesEnabled does not throw when true', async () => {
      const { assertFreshQueriesEnabled } = await loadUtilsModule();

      expect(() => assertFreshQueriesEnabled(true)).not.toThrow();
    });

    test('acquireFreshQuerySlot throws TooManyFreshQueries when max reached', async () => {
      const { acquireFreshQuerySlot } = await loadUtilsModule();

      // consume the only allowed slot by default when maxConcurrent=1
      const release1 = acquireFreshQuerySlot(1);

      expect(() => acquireFreshQuerySlot(1)).toThrow(
        'Too many concurrent fresh queries (limit 1)',
      );

      release1();
      expect(() => acquireFreshQuerySlot(1)).not.toThrow();
    });
  });

  describe('getWindowDate', () => {
    test('returns date 1 day ago for "day" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      jest.useFakeTimers({ now: new Date(2026, 3, 1, 10, 20, 30, 456) });

      const now = new Date();
      const expected = new Date(now);
      expected.setDate(expected.getDate() - 1);
      const result = getWindowDate('day');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getTime()).toBe(expected.getTime());

      jest.useRealTimers();
    });

    test('returns date 7 days ago for "week" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      jest.useFakeTimers({ now: new Date(2026, 3, 1, 10, 20, 30, 456) });

      const now = new Date();
      const expected = new Date(now);
      expected.setDate(expected.getDate() - 7);
      const result = getWindowDate('week');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getTime()).toBe(expected.getTime());

      jest.useRealTimers();
    });

    test('returns date 1 month ago for "month" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      jest.useFakeTimers({ now: new Date(2026, 2, 31, 12, 34, 56, 789) });

      const now = new Date();
      const expected = new Date(now);
      expected.setMonth(expected.getMonth() - 1);
      const result = getWindowDate('month');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getTime()).toBe(expected.getTime());

      jest.useRealTimers();
    });

    test('returns date 1 year ago for "year" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const now = new Date();
      const expected = new Date(now);
      expected.setFullYear(expected.getFullYear() - 1);
      const result = getWindowDate('year');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.toDateString()).toBe(expected.toDateString());
    });

    test('returns undefined for invalid windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const result = getWindowDate('invalid');

      expect(result).toBeUndefined();
    });

    test('returns undefined for empty string windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const result = getWindowDate('');

      expect(result).toBeUndefined();
    });

    test('returns undefined for undefined windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const result = getWindowDate(undefined);

      expect(result).toBeUndefined();
    });

    test('returns undefined for null windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const result = getWindowDate(null);

      expect(result).toBeUndefined();
    });

    test('handles month boundary correctly (31st to 28th/29th/30th/31st)', async () => {
      const { getWindowDate } = await loadUtilsModule();

      jest.useFakeTimers({ now: new Date(2024, 0, 31) });

      const result = getWindowDate('month');

      expect(result).toBeInstanceOf(Date);
      expect(result.getFullYear()).toBe(2023);
      expect(result.getMonth()).toBe(11); // December
      expect(result.getDate()).toBe(31);

      jest.useRealTimers();
    });

    test('handles leap year correctly for February', async () => {
      const { getWindowDate } = await loadUtilsModule();

      jest.useFakeTimers({ now: new Date(2024, 1, 29) });

      const result = getWindowDate('month');

      expect(result).toBeInstanceOf(Date);
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(0); // January
      expect(result.getDate()).toBe(29);

      jest.useRealTimers();
    });
  });

  describe('processFetchSize', () => {
    let processFetchSize;

    beforeAll(async () => {
      const utils = await loadUtilsModule();
      processFetchSize = utils.processFetchSize;
    });

    test('returns a singular unit with amount 1 for a single token', () => {
      expect(processFetchSize('day')).toEqual({ unit: 'day', amount: 1 });
      expect(processFetchSize('days')).toEqual({ unit: 'day', amount: 1 });
    });

    test('parses explicit quantity and normalizes plural units', () => {
      expect(processFetchSize('2 hours')).toEqual({ unit: 'hour', amount: 2 });
      expect(processFetchSize('3 minutes')).toEqual({
        unit: 'minute',
        amount: 3,
      });
    });

    test('throws for malformed time sizes', () => {
      expect(() => processFetchSize('1 day extra')).toThrow(
        'Invalid time size',
      );
      expect(() => processFetchSize('')).toThrow('Invalid unit in time size');
    });

    test('throws for non-integer amounts', () => {
      expect(() => processFetchSize('1.5 days')).toThrow(
        'Invalid amount in time size',
      );
    });
  });

  describe('parseUploadedFile', () => {
    test('parses CSV files with comma delimiter', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const parsed = parseUploadedFile(
        Buffer.from('device,status\nsensor-a,ok\n'),
        'text/csv',
        'data.csv',
      );

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a,ok');
    });

    test('parses CSV files from Uint8Array input', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const parsed = parseUploadedFile(
        new TextEncoder().encode('device,status\nsensor-a,ok\n'),
        'text/csv',
        'data.csv',
      );

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a,ok');
    });

    test('parses CSV files from wrapped buffer objects', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const raw = Buffer.from('device,status\nsensor-a,ok\n');
      const wrapped = {
        buffer: raw.buffer,
        byteOffset: raw.byteOffset,
        byteLength: raw.byteLength,
      };

      const parsed = parseUploadedFile(wrapped, 'text/csv', 'data.csv');

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a,ok');
    });

    test('parses CSV files from string input', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const parsed = parseUploadedFile(
        'device,status\nsensor-a,ok\n',
        'text/csv',
        'data.csv',
      );

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a,ok');
    });

    test('rejects CSV files with an empty header row', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      expect(() =>
        parseUploadedFile(Buffer.from(',,\n1,2,3\n'), 'text/csv', 'data.csv'),
      ).toThrow('Invalid CSV format: CSV header row is empty');
    });

    test('rejects empty CSV files', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      expect(() =>
        parseUploadedFile(Buffer.from(''), 'text/csv', 'data.csv'),
      ).toThrow('Invalid CSV format: CSV file has no header row');
    });

    test('parses CSV files with semicolon delimiter', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const parsed = parseUploadedFile(
        Buffer.from('device;status\nsensor-a;ok\n'),
        'text/csv',
        'data.csv',
      );

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a;ok');
    });

    test('parses CSV files with UTF-8 BOM', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const parsed = parseUploadedFile(
        Buffer.concat([
          Buffer.from([0xef, 0xbb, 0xbf]),
          Buffer.from('device,status\nsensor-a,ok\n'),
        ]),
        'text/csv',
        'data.csv',
      );

      expect(parsed.headers).toEqual(['device', 'status']);
      expect(parsed.csvContent).toContain('sensor-a,ok');
    });

    test('rejects malformed CSV with inconsistent columns', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      expect(() =>
        parseUploadedFile(
          Buffer.from('value1,value2\nonly_one_column\n'),
          'text/csv',
          'broken.csv',
        ),
      ).toThrow('Invalid CSV format:');
    });

    test('parses XLSX files combining rows from multiple sheets', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const workbook = xlsx.utils.book_new();
      const mainSheet = xlsx.utils.aoa_to_sheet([
        ['device', 'reading'],
        ['sensor-a', 10],
      ]);
      const otherSheet = xlsx.utils.aoa_to_sheet([
        ['id', 'value'],
        ['row-b', 20],
      ]);
      xlsx.utils.book_append_sheet(workbook, mainSheet, 'Main');
      xlsx.utils.book_append_sheet(workbook, otherSheet, 'Other');

      const xlsxBuffer = xlsx.write(workbook, {
        type: 'buffer',
        bookType: 'xlsx',
      });

      const parsed = parseUploadedFile(
        xlsxBuffer,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'data.xlsx',
      );

      expect(parsed.headers).toEqual(
        expect.arrayContaining(['device', 'reading', 'id', 'value']),
      );
      expect(parsed.csvContent).toContain('sensor-a');
      expect(parsed.csvContent).toContain('row-b');
    });

    test('parses XLSX files through the manual cell fallback', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      const readSpy = jest.spyOn(xlsx, 'read').mockReturnValue({
        SheetNames: ['Sheet1'],
        Sheets: {
          Sheet1: {
            '!ref': 'A1:C3',
            A1: { v: 'device' },
            B1: { w: 'reading' },
            A2: { v: 'sensor-a' },
            B2: { w: '10' },
            A3: {},
            B3: {},
            C3: {},
          },
        },
      });
      const sheetToJsonSpy = jest
        .spyOn(xlsx.utils, 'sheet_to_json')
        .mockReturnValue([]);

      try {
        const parsed = parseUploadedFile(
          Buffer.from([0x50, 0x4b, 0x03, 0x04]),
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          'data.xlsx',
        );

        expect(parsed.headers).toEqual(['device', 'reading']);
        expect(parsed.csvContent).toContain('sensor-a,10');
      } finally {
        readSpy.mockRestore();
        sheetToJsonSpy.mockRestore();
      }
    });

    test('rejects unsupported upload media types', async () => {
      const { parseUploadedFile } = await loadUtilsModule();

      expect(() =>
        parseUploadedFile(Buffer.from('hello'), 'text/plain', 'file.txt'),
      ).toThrow('Only CSV, XLS, or XLSX files are allowed');
    });
  });

  describe('convertRefreshIntervalToMs', () => {
    let convertRefreshIntervalToMs;

    beforeAll(async () => {
      const utils = await loadUtilsModule();
      convertRefreshIntervalToMs = utils.convertRefreshIntervalToMs;
    });

    describe('invalid inputs', () => {
      test('returns null for undefined', () => {
        expect(convertRefreshIntervalToMs(undefined)).toBeNull();
      });

      test('returns null for null', () => {
        expect(convertRefreshIntervalToMs(null)).toBeNull();
      });

      test('returns null for non-string values', () => {
        expect(convertRefreshIntervalToMs(123)).toBeNull();
        expect(convertRefreshIntervalToMs({})).toBeNull();
        expect(convertRefreshIntervalToMs([])).toBeNull();
      });

      test('returns null for empty string', () => {
        expect(convertRefreshIntervalToMs('')).toBeNull();
        expect(convertRefreshIntervalToMs('   ')).toBeNull();
      });

      test('returns null for invalid format', () => {
        expect(convertRefreshIntervalToMs('abc')).toBeNull();
        expect(convertRefreshIntervalToMs('10 lightyears')).toBeNull();
        expect(convertRefreshIntervalToMs('minutes 5')).toBeNull();
        expect(convertRefreshIntervalToMs('not_an_interval')).toBeNull();
      });
    });

    describe('human-readable intervals', () => {
      test('parses seconds', () => {
        expect(convertRefreshIntervalToMs('1 second')).toBe(1000);
        expect(convertRefreshIntervalToMs('10 seconds')).toBe(10000);
      });

      test('parses minutes', () => {
        expect(convertRefreshIntervalToMs('1 minute')).toBe(60 * 1000);
        expect(convertRefreshIntervalToMs('5 minutes')).toBe(5 * 60 * 1000);
      });

      test('parses hours', () => {
        expect(convertRefreshIntervalToMs('1 hour')).toBe(60 * 60 * 1000);
        expect(convertRefreshIntervalToMs('2 hours')).toBe(2 * 60 * 60 * 1000);
      });

      test('parses days', () => {
        expect(convertRefreshIntervalToMs('1 day')).toBe(24 * 60 * 60 * 1000);
      });

      test('parses weeks', () => {
        expect(convertRefreshIntervalToMs('1 week')).toBe(
          7 * 24 * 60 * 60 * 1000,
        );
      });

      test('parses months (30 days)', () => {
        expect(convertRefreshIntervalToMs('1 month')).toBe(
          30 * 24 * 60 * 60 * 1000,
        );
      });

      test('parses years (365 days)', () => {
        expect(convertRefreshIntervalToMs('1 year')).toBe(
          365 * 24 * 60 * 60 * 1000,
        );
      });
    });

    describe('normalization', () => {
      test('handles uppercase input', () => {
        expect(convertRefreshIntervalToMs('1 MINUTE')).toBe(60 * 1000);
      });

      test('handles extra whitespace', () => {
        expect(convertRefreshIntervalToMs('   2   hours   ')).toBe(
          2 * 60 * 60 * 1000,
        );
      });

      test('handles singular and plural forms', () => {
        expect(convertRefreshIntervalToMs('1 minute')).toBe(60 * 1000);
        expect(convertRefreshIntervalToMs('2 minutes')).toBe(2 * 60 * 1000);
      });
    });

    describe('cron fallback', () => {
      test('returns interval between two cron executions', () => {
        const nextMock = jest
          .fn()
          .mockReturnValueOnce({ getTime: () => 1000 })
          .mockReturnValueOnce({ getTime: () => 4000 });

        cronParserMock.parse.mockReturnValue({
          next: nextMock,
        });

        expect(convertRefreshIntervalToMs('* * * * *')).toBe(3000);
      });
    });
  });
});
