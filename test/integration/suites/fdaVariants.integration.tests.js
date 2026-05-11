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

export function registerFdaVariantsIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  buildFdaDataUrl,
}) {
  describe('FDA variants', () => {
    test('POST /fdas?defaultDataAccess=false creates FDA without default DA', async () => {
      const baseUrl = getBaseUrl();
      const disabledFdaId = 'fda_default_da_disabled';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas?defaultDataAccess=false`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: disabledFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'default DA disabled test',
          },
        });

        expect(createFda.status).toBe(202);
        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: disabledFdaId,
        });

        expect(completedFDA?.das || {}).toEqual({});
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${disabledFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with cached=false creates an only-fresh FDA without DAs', async () => {
      const baseUrl = getBaseUrl();
      const onlyFreshFdaId = 'fda_only_fresh';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: onlyFreshFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'only fresh FDA',
            cached: false,
          },
        });

        expect(createFda.status).toBe(202);

        const storedFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}`,
          headers: { 'Fiware-Service': service },
        });

        expect(storedFda.status).toBe(200);
        expect(storedFda.json.cached).toBe(false);
        expect(storedFda.json.das || {}).toEqual({});

        const directQueryRes = await httpReq({
          method: 'GET',
          url: buildFdaDataUrl(baseUrl, servicePath, onlyFreshFdaId),
          headers: { 'Fiware-Service': service },
        });

        expect(directQueryRes.status).toBe(200);
        expect(directQueryRes.json.map((row) => row.name)).toEqual([
          'ana',
          'bob',
          'carlos',
        ]);

        const createDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: 'da_should_fail',
            description: 'must fail',
            query: 'SELECT id ORDER BY id',
          },
        });

        expect(createDa.status).toBe(409);
        expect(createDa.json.error).toBe('FDAOnlyFresh');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });
  });
}
