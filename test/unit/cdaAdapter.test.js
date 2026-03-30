// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefonica Soluciones
// de Informatica y Comunicaciones de Espana, S.A.U (hereinafter TSOL) and is protected
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

const executeQueryMock = jest.fn();

async function loadCdaAdapterModule() {
  jest.resetModules();
  executeQueryMock.mockReset();

  await jest.unstable_mockModule('../../src/lib/fda.js', () => ({
    executeQuery: executeQueryMock,
  }));

  return import('../../src/lib/compat/cdaAdapter.js');
}

describe('cda adapter', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('throws when unsupported param_not_ keys are provided', async () => {
    const { handleCdaQuery } = await loadCdaAdapterModule();

    await expect(
      handleCdaQuery({
        body: {
          path: '/public/svc',
          dataAccessId: 'daA',
          param_not_flag: true,
        },
      }),
    ).rejects.toThrow('param_not_ is not supported yet: param_not_flag');
  });

  test('returns empty CDA payload when no rows are returned', async () => {
    const { handleCdaQuery } = await loadCdaAdapterModule();

    executeQueryMock.mockResolvedValueOnce([]);

    const result = await handleCdaQuery({
      body: {
        path: '/public/svc',
        dataAccessId: 'daA',
        pageStart: '5',
        pageSize: '2',
      },
    });

    expect(result).toEqual({
      metadata: [],
      resultset: [],
      queryInfo: {
        pageStart: 5,
        pageSize: 2,
        totalRows: 0,
      },
    });
    expect(executeQueryMock).toHaveBeenCalledWith(
      expect.objectContaining({
        service: 'svc',
        visibility: 'public',
        servicePath: '/public',
        params: expect.objectContaining({
          fdaId: 'daA',
          daId: 'daA',
          pageStart: 5,
          pageSize: 2,
        }),
      }),
    );
  });

  test('returns raw rows when outputType is not json', async () => {
    const { handleCdaQuery } = await loadCdaAdapterModule();

    const rows = [{ col1: 'a', col2: 'b' }];
    executeQueryMock.mockResolvedValueOnce(rows);

    const result = await handleCdaQuery({
      body: {
        path: '/public/svc',
        dataAccessId: 'daA',
      },
      outputType: 'csv',
    });

    expect(result).toBe(rows);
    expect(executeQueryMock).toHaveBeenCalledWith(
      expect.objectContaining({
        service: 'svc',
        visibility: 'public',
        servicePath: '/public',
      }),
    );
  });
});
