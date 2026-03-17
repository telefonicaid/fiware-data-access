import { describe, expect, test } from '@jest/globals';
import {
  assertFreshQueriesEnabled,
  acquireFreshQuerySlot,
} from '../../src/lib/utils/utils.js';

describe('fresh query helpers', () => {
  test('assertFreshQueriesEnabled throws when fresh queries are disabled', () => {
    expect(() => assertFreshQueriesEnabled(false)).toThrow(
      'Fresh query mode is disabled in this instance',
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
});
