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

import { describe, test, expect } from '@jest/globals';

export function registerFdaTimeColumnIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
}) {
  describe('Time column validation in strict mode', () => {
    test('POST /fdas with timeColumn in SELECT (unqualified) succeeds', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_unqualified';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: 'SELECT timeinstant, id, name FROM public.users ORDER BY id',
            description: 'FDA with timeColumn unqualified in SELECT',
            timeColumn: 'timeinstant',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.timeColumn).toBe('timeinstant');
        expect(completedFDA.validationMode).toBe('strict');
        expect(completedFDA.schema).toBeDefined();
        expect(
          completedFDA.schema.some((col) => col.name === 'timeinstant'),
        ).toBe(true);

        const query = completedFDA.query;
        const timeColumnMatches = query.match(/timeinstant/g) || [];
        expect(timeColumnMatches.length).toBe(1);

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).toHaveProperty('timeinstant');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn in SELECT (qualified with table alias) succeeds', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_qualified';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query:
              'SELECT u.timeinstant, u.id, u.name FROM public.users u ORDER BY u.id',
            description: 'FDA with timeColumn qualified in SELECT',
            timeColumn: 'timeinstant',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.timeColumn).toBe('timeinstant');
        expect(completedFDA.validationMode).toBe('strict');
        expect(
          completedFDA.schema.some((col) => col.name === 'timeinstant'),
        ).toBe(true);

        const query = completedFDA.query;
        const timeColumnMatches = query.match(/timeinstant/g) || [];
        expect(timeColumnMatches.length).toBe(1);

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn in SELECT with alias succeeds', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_alias';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query:
              'SELECT timeinstant AS ts, id, name FROM public.users ORDER BY id',
            description: 'FDA with timeColumn with alias in SELECT',
            timeColumn: 'ts',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.schema.some((col) => col.name === 'ts')).toBe(true);
        expect(completedFDA.timeColumn).toBe('ts');

        const query = completedFDA.query;
        expect(query).toBe(
          'SELECT timeinstant AS ts, id, name FROM public.users ORDER BY id',
        );

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).toHaveProperty('ts');
        expect(daResponse.json[0]).not.toHaveProperty('timeinstant');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with SELECT * includes timeColumn implicitly succeeds', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_select_star';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: 'SELECT * FROM public.users ORDER BY id',
            description: 'FDA with SELECT * includes timeColumn implicitly',
            timeColumn: 'timeinstant',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(
          completedFDA.schema.some((col) => col.name === 'timeinstant'),
        ).toBe(true);

        expect(completedFDA.query).toBe(
          'SELECT * FROM public.users ORDER BY id',
        );

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).toHaveProperty('timeinstant');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn NOT in SELECT fails with clear error message in strict mode', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_missing';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'FDA without timeColumn in SELECT',
          timeColumn: 'timeinstant',
          validationMode: 'strict',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 1 * *',
              fetchSize: 'month',
            },
          },
          objStgConf: {
            partition: 'month',
          },
        },
      });

      expect(createFda.status).toBe(400);
      expect(createFda.json.error).toBe('InvalidParam');
      expect(createFda.json.description).toContain(
        '"timeinstant" is not present in the SELECT clause',
      );
      expect(createFda.json.description).toContain(
        'Available columns: "id", "name", "age"',
      );

      await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
    });

    test('POST /fdas with timeColumn NOT in SELECT but in unchecked mode succeeds (no validation)', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_unchecked';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: 'SELECT id, name, age FROM public.users ORDER BY id',
            description: 'FDA without timeColumn in unchecked mode',
            timeColumn: 'timeinstant',
            validationMode: 'unchecked',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.validationMode).toBe('unchecked');
        expect(completedFDA.schema).toBeUndefined();

        expect(completedFDA.das || {}).toEqual({});

        expect(completedFDA.query).toBe(
          'SELECT id, name, age FROM public.users ORDER BY id',
        );
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn case-insensitive matching works', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_case';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: 'SELECT TIMEINSTANT, id, name FROM public.users ORDER BY id',
            description: 'FDA with timeColumn in uppercase in SELECT',
            timeColumn: 'timeinstant',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(
          completedFDA.schema.some((col) => col.name === 'timeinstant'),
        ).toBe(true);

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).toHaveProperty('timeinstant');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn via function with alias passes validation', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_function';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query:
              'SELECT EXTRACT(YEAR FROM timeinstant) AS timeyear, id, name FROM public.users ORDER BY id',
            description: 'FDA with timeColumn via function with alias',
            timeColumn: 'timeyear',
            validationMode: 'strict',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 0 1 * *',
                fetchSize: 'month',
              },
            },
            objStgConf: {
              partition: 'month',
            },
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.schema.some((col) => col.name === 'timeyear')).toBe(
          true,
        );

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).toHaveProperty('timeyear');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with timeColumn NOT in SELECT but no refreshPolicy/partition succeeds in strict mode', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'fda_timecolumn_no_refresh';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: 'SELECT id, name, age FROM public.users ORDER BY id',
            description: 'FDA with timeColumn but no refreshPolicy/partition',
            timeColumn: 'timeinstant',
            validationMode: 'strict',
            cached: true,
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        expect(completedFDA.timeColumn).toBe('timeinstant');
        expect(completedFDA.schema).toBeUndefined();

        const daResponse = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/default/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(daResponse.status).toBe(200);
        expect(daResponse.json[0]).not.toHaveProperty('timeinstant');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });
  });
}
