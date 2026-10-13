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
  getPartitionInterval,
  resolveTimeRange,
  selectPartitionFiles,
} from '../../src/lib/utils/partitionPruning.js';

const TIME_RANGE = { start: 'start', finish: 'finish' };

function dayKey(year, month, day) {
  return `sp/fda.parquet/year=${year}/month=${month}/day=${day}/data_0.parquet`;
}

describe('partitionPruning', () => {
  describe('resolveTimeRange', () => {
    test('returns null when the DA declares no timeRange', () => {
      expect(resolveTimeRange(undefined, { start: '2025-06-01' })).toBeNull();
    });

    test('returns null when no bound has a value', () => {
      expect(
        resolveTimeRange(TIME_RANGE, { start: null, finish: undefined }),
      ).toBeNull();
      expect(resolveTimeRange(TIME_RANGE, { start: '' })).toBeNull();
    });

    test('widens both bounds by one day', () => {
      expect(
        resolveTimeRange(TIME_RANGE, {
          start: '2025-06-01T00:00:00Z',
          finish: '2025-06-04T00:00:00Z',
        }),
      ).toEqual({
        from: new Date('2025-05-31T00:00:00Z'),
        to: new Date('2025-06-05T00:00:00Z'),
      });
    });

    test('supports open-ended ranges', () => {
      expect(
        resolveTimeRange(TIME_RANGE, { start: '2025-06-01T00:00:00Z' }),
      ).toEqual({ from: new Date('2025-05-31T00:00:00Z'), to: null });
      expect(
        resolveTimeRange(
          { finish: 'until' },
          {
            until: new Date('2025-06-04T00:00:00Z'),
          },
        ),
      ).toEqual({ from: null, to: new Date('2025-06-05T00:00:00Z') });
    });

    test('returns null when a bound cannot be read as a date', () => {
      expect(
        resolveTimeRange(TIME_RANGE, {
          start: 'not-a-date',
          finish: '2025-06-04T00:00:00Z',
        }),
      ).toBeNull();
      expect(resolveTimeRange(TIME_RANGE, { start: 20250601 })).toBeNull();
    });
  });

  describe('getPartitionInterval', () => {
    test('reads day, month and year partitions', () => {
      expect(getPartitionInterval(dayKey(2025, 6, 30))).toEqual({
        start: new Date('2025-06-30T00:00:00Z'),
        end: new Date('2025-07-01T00:00:00Z'),
      });
      expect(
        getPartitionInterval(
          'sp/fda.parquet/year=2025/month=12/data_0.parquet',
        ),
      ).toEqual({
        start: new Date('2025-12-01T00:00:00Z'),
        end: new Date('2026-01-01T00:00:00Z'),
      });
      expect(
        getPartitionInterval('sp/fda.parquet/year=2025/data_0.parquet'),
      ).toEqual({
        start: new Date('2025-01-01T00:00:00Z'),
        end: new Date('2026-01-01T00:00:00Z'),
      });
    });

    test('reads week partitions as Monday-based %W weeks', () => {
      expect(
        getPartitionInterval(
          'sp/fda.parquet/year=2025/week=2025-22/data_0.parquet',
        ),
      ).toEqual({
        start: new Date('2025-06-02T00:00:00Z'),
        end: new Date('2025-06-09T00:00:00Z'),
      });
      expect(
        getPartitionInterval(
          'sp/fda.parquet/year=2025/week=2025-00/data_0.parquet',
        ),
      ).toEqual({
        start: new Date('2024-12-30T00:00:00Z'),
        end: new Date('2025-01-06T00:00:00Z'),
      });
      expect(
        getPartitionInterval(
          'sp/fda.parquet/year=2024/week=2024-01/data_0.parquet',
        ),
      ).toEqual({
        start: new Date('2024-01-01T00:00:00Z'),
        end: new Date('2024-01-08T00:00:00Z'),
      });
    });

    test('returns null for keys without partition values', () => {
      expect(getPartitionInterval('sp/fda.parquet/data_0.parquet')).toBeNull();
      expect(getPartitionInterval(undefined)).toBeNull();
    });
  });

  describe('selectPartitionFiles', () => {
    const keys = [
      dayKey(2024, 12, 30),
      dayKey(2024, 12, 31),
      dayKey(2025, 1, 1),
      dayKey(2025, 1, 2),
      dayKey(2025, 1, 3),
    ];

    test('keeps the partitions overlapping a range across years', () => {
      const range = {
        from: new Date('2024-12-31T12:00:00Z'),
        to: new Date('2025-01-01T12:00:00Z'),
      };

      expect(selectPartitionFiles(keys, range)).toEqual([
        dayKey(2024, 12, 31),
        dayKey(2025, 1, 1),
      ]);
    });

    test('supports open-ended ranges', () => {
      expect(
        selectPartitionFiles(keys, {
          from: new Date('2025-01-02T00:00:00Z'),
          to: null,
        }),
      ).toEqual([dayKey(2025, 1, 2), dayKey(2025, 1, 3)]);
      expect(
        selectPartitionFiles(keys, {
          from: null,
          to: new Date('2024-12-30T23:00:00Z'),
        }),
      ).toEqual([dayKey(2024, 12, 30)]);
    });

    test('keeps unreadable parquet keys and drops other objects', () => {
      const range = { from: new Date('2030-01-01T00:00:00Z'), to: null };

      expect(
        selectPartitionFiles(
          [...keys, 'sp/fda.parquet/data_0.parquet', 'sp/fda.parquet/_SUCCESS'],
          range,
        ),
      ).toEqual(['sp/fda.parquet/data_0.parquet']);
    });
  });
});
