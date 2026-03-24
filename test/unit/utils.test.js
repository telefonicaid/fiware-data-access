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

async function loadUtilsModule() {
  jest.resetModules();

  loggerMock.debug.mockClear();
  loggerMock.info.mockClear();
  loggerMock.error.mockClear();

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  return import('../../src/lib/utils/utils.js');
}

describe('utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('getWindowDate', () => {
    test('returns date 1 day ago for "day" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const now = new Date();
      const result = getWindowDate('day');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getDate()).toBe(now.getDate() - 1);
    });

    test('returns date 7 days ago for "week" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const now = new Date();
      const result = getWindowDate('week');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getDate()).toBe(now.getDate() - 7);
    });

    test('returns date 1 month ago for "month" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const now = new Date();
      const result = getWindowDate('month');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getMonth()).toBe(now.getMonth() - 1);
    });

    test('returns date 1 year ago for "year" windowSize', async () => {
      const { getWindowDate } = await loadUtilsModule();

      const now = new Date();
      const result = getWindowDate('year');

      expect(result).toBeInstanceOf(Date);
      expect(result.getTime()).toBeLessThan(now.getTime());
      expect(result.getFullYear()).toBe(now.getFullYear() - 1);
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
});
