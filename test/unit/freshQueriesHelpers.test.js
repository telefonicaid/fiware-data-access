import { describe, expect, test } from '@jest/globals';
import {
  convertBigInt,
  validateAllowedFieldsBody,
  parseBooleanQueryParam,
  assertFreshQueriesEnabled,
  acquireFreshQuerySlot,
} from '../../src/lib/utils/utils.js';

describe('fresh query helpers', () => {
  test('assertFreshQueriesEnabled throws when fresh queries are disabled', () => {
    expect(() => assertFreshQueriesEnabled(false)).toThrow(
      'Fresh query mode is disabled in this instance',
    );
  });

  test('convertBigInt converts bigint values recursively', () => {
    const payload = {
      id: 7n,
      nested: {
        values: [1n, { count: 2n }],
      },
    };

    expect(convertBigInt(payload)).toEqual({
      id: 7,
      nested: {
        values: [1, { count: 2 }],
      },
    });
  });

  test('validateAllowedFieldsBody throws when body includes invalid fields', () => {
    expect(() =>
      validateAllowedFieldsBody({ query: 'SELECT 1', forbidden: true }, [
        'query',
        'description',
      ]),
    ).toThrow('Invalid fields in request body, check your request');
  });

  test('parseBooleanQueryParam parses valid values and rejects invalid ones', () => {
    expect(parseBooleanQueryParam(undefined, 'fresh')).toBe(false);
    expect(parseBooleanQueryParam(true, 'fresh')).toBe(true);
    expect(parseBooleanQueryParam(' 1 ', 'fresh')).toBe(true);
    expect(parseBooleanQueryParam('0', 'fresh')).toBe(false);

    expect(() => parseBooleanQueryParam(10, 'fresh')).toThrow(
      'Query param "fresh" must be a boolean.',
    );
    expect(() => parseBooleanQueryParam('yes', 'fresh')).toThrow(
      'Query param "fresh" must be a boolean.',
    );
  });

  test('acquireFreshQuerySlot enforces the configured limit and release is idempotent', () => {
    const release = acquireFreshQuerySlot(1);

    expect(() => acquireFreshQuerySlot(1)).toThrow(
      'Too many concurrent fresh queries (limit 1)',
    );

    release();
    release();

    const releaseAgain = acquireFreshQuerySlot(1);
    releaseAgain();
  });

  test('acquireFreshQuerySlot uses fallback and clamps invalid limits', () => {
    const releases = [
      acquireFreshQuerySlot('not-a-number'),
      acquireFreshQuerySlot('not-a-number'),
      acquireFreshQuerySlot('not-a-number'),
      acquireFreshQuerySlot('not-a-number'),
      acquireFreshQuerySlot('not-a-number'),
    ];

    expect(() => acquireFreshQuerySlot('not-a-number')).toThrow(
      'Too many concurrent fresh queries (limit 5)',
    );

    releases.forEach((release) => release());

    const releaseClamped = acquireFreshQuerySlot(0);
    expect(() => acquireFreshQuerySlot(0)).toThrow(
      'Too many concurrent fresh queries (limit 1)',
    );

    releaseClamped();
  });
});
