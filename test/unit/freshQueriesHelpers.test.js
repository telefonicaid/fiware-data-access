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
  normalizeForSerialization,
  validateAllowedFieldsBody,
  validateForbiddenFieldsQuery,
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

  test('normalizeForSerialization converts bigint values recursively', () => {
    const payload = {
      id: 7n,
      nested: {
        values: [1n, { count: 2n }],
      },
    };

    expect(normalizeForSerialization(payload)).toEqual({
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

  test('validateForbiddenFieldsQuery throws when query includes forbidden fields', () => {
    expect(() =>
      validateForbiddenFieldsQuery({ minAge: '25', outputType: 'csv' }, [
        'outputType',
      ]),
    ).toThrow('Invalid fields in request query, check your request');
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
