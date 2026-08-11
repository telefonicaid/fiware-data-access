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

import { jest, describe, beforeEach, test, expect } from '@jest/globals';

jest.unstable_mockModule('uuid', () => ({
  v4: jest.fn(),
}));

jest.unstable_mockModule('logops', () => ({
  default: {
    getContext: jest.fn().mockReturnValue({ op: 'test' }),
    child: jest.fn().mockImplementation((fields) => fields),
  },
}));

const { createChildLogger } = await import('../../src/lib/utils/logger.js');
const { v4: uuidv4 } = await import('uuid');

describe('createChildLogger', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should use provided corr when passed', () => {
    uuidv4.mockReturnValue('should-not-be-used');
    const result = createChildLogger({ corr: 'my-custom-corr' });
    expect(uuidv4).toHaveBeenCalledTimes(1);
    expect(result.corr).toBe('my-custom-corr');
  });

  test('should generate a UUID when corr is undefined', () => {
    uuidv4.mockReturnValue('generated-uuid-123');
    const result = createChildLogger({});
    expect(uuidv4).toHaveBeenCalledTimes(2);
    expect(result.corr).toBe('generated-uuid-123');
  });

  test('should generate a UUID when corr is null', () => {
    uuidv4.mockReturnValue('generated-uuid-123');
    const result = createChildLogger({ corr: null });
    expect(uuidv4).toHaveBeenCalledTimes(2);
    expect(result.corr).toBe('generated-uuid-123');
  });

  test('should generate a UUID when corr is empty string', () => {
    uuidv4.mockReturnValue('generated-uuid-123');
    const result = createChildLogger({ corr: '' });
    expect(uuidv4).toHaveBeenCalledTimes(2);
    expect(result.corr).toBe('generated-uuid-123');
  });
});
