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

  return import('../../src/lib/utils/utils.js');
}

describe('utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
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

  describe('getFinalQuery', () => {
    let getFinalQuery;

    beforeAll(async () => {
      const utils = await loadUtilsModule();
      getFinalQuery = utils.getFinalQuery;
    });

    describe('invalid inputs', () => {
      test('throws for invalid timeColumn (special chars)', () => {
        expect(() =>
          getFinalQuery('SELECT a FROM table', 'time-column'),
        ).toThrow('Invalid time column name');
      });

      test('throws for invalid timeColumn (empty)', () => {
        expect(() => getFinalQuery('SELECT a FROM table', '')).toThrow(
          'Invalid time column name',
        );
      });

      test('throws if query has no SELECT', () => {
        expect(() => getFinalQuery('DELETE FROM table', 'time')).toThrow(
          'Missing SELECT',
        );
      });
    });

    describe('no modification cases', () => {
      test('returns query if SELECT *', () => {
        const query = 'SELECT * FROM table';
        expect(getFinalQuery(query, 'time')).toBe(query);
      });

      test('returns query if timeColumn already present', () => {
        const query = 'SELECT time, value FROM table';
        expect(getFinalQuery(query, 'time')).toBe(query);
      });

      test('returns query if timeColumn already present (middle)', () => {
        const query = 'SELECT value, time, other FROM table';
        expect(getFinalQuery(query, 'time')).toBe(query);
      });
    });

    describe('insertion behavior', () => {
      test('inserts timeColumn after SELECT', () => {
        const result = getFinalQuery('SELECT value FROM table', 'time');

        expect(result).toBe('SELECT time, value FROM table');
      });

      test('handles multiple columns', () => {
        const result = getFinalQuery('SELECT value, other FROM table', 'time');

        expect(result).toBe('SELECT time, value, other FROM table');
      });

      test('handles extra whitespace after SELECT', () => {
        const result = getFinalQuery('SELECT   value FROM table', 'time');

        expect(result).toBe('SELECT   time, value FROM table');
      });

      test('handles lowercase select/from', () => {
        const result = getFinalQuery('select value from table', 'time');

        expect(result).toBe('select time, value from table');
      });
    });
  });
});
