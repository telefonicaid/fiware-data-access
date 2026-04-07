import { describe, expect, test } from '@jest/globals';
import {
  normalizeScopedServicePath,
  getFDAStoragePath,
} from '../../src/lib/utils/fdaScope.js';

describe('fdaScope utils', () => {
  test('normalizeScopedServicePath throws when servicePath is missing', () => {
    expect(() => normalizeScopedServicePath(undefined)).toThrow(
      'servicePath is required',
    );
  });

  test('normalizeScopedServicePath throws when servicePath is blank', () => {
    expect(() => normalizeScopedServicePath('   ')).toThrow(
      'servicePath is required',
    );
  });

  test('getFDAStoragePath builds scoped path for root servicePath', () => {
    expect(getFDAStoragePath('fdaA', '/')).toBe('_root/fdaA');
  });

  test('getFDAStoragePath builds scoped path for non-root servicePath', () => {
    expect(getFDAStoragePath('fdaA', '/public')).toBe('public/fdaA');
  });
});
