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
    test('DELETE /fdas/:fdaId only removes the targeted FDA when another FDA shares its prefix', async () => {
      const baseUrl = getBaseUrl();
      const prefixFdaId = 'fda_test';
      const siblingPrefixFdaId = 'fda_test_1';
      const siblingDaId = 'da_sibling_test';

      async function deleteFdaIfPresent(fdaId) {
        const deleteRes = await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect([204, 404]).toContain(deleteRes.status);
      }

      try {
        const createFirstFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: prefixFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'prefix delete regression FDA 1',
          },
        });

        expect(createFirstFda.status).toBe(202);

        const createSiblingFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: siblingPrefixFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'prefix delete regression FDA 2',
          },
        });

        expect(createSiblingFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: prefixFdaId,
        });
        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: siblingPrefixFdaId,
        });

        const createSiblingDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${siblingPrefixFdaId}/das`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: siblingDaId,
            query: 'SELECT id, name, age, timeinstant, authorized',
          },
        });

        expect(createSiblingDa.status).toBe(200);

        const listRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(listRes.status).toBe(200);
        expect(Array.isArray(listRes.json)).toBe(true);
        expect(listRes.json.some((fda) => fda.id === prefixFdaId)).toBe(true);
        expect(listRes.json.some((fda) => fda.id === siblingPrefixFdaId)).toBe(
          true,
        );

        const deleteFirstFda = await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${prefixFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(deleteFirstFda.status).toBe(204);

        const siblingRead = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${siblingPrefixFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(siblingRead.status).toBe(200);

        const siblingDataRead = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${siblingPrefixFdaId}/das/${siblingDaId}/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(siblingDataRead.status).toBe(200);
        expect(Array.isArray(siblingDataRead.json)).toBe(true);
        expect(siblingDataRead.json).toHaveLength(3);

        const deleteSiblingFda = await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${siblingPrefixFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(deleteSiblingFda.status).toBe(204);
      } finally {
        await deleteFdaIfPresent(prefixFdaId);
        await deleteFdaIfPresent(siblingPrefixFdaId);
      }
    });

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

    test('Fiware-ServicePath multipath only uses first token for FDA scoping', async () => {
      const baseUrl = getBaseUrl();

      const fdaId = 'fda_multipath_scope_test';

      const multiServicePath = servicePath + '/B/C/D';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': multiServicePath,
          },
          body: {
            id: fdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'multipath servicePath regression test',
          },
        });

        expect(createFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
        });

        const listRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': multiServicePath,
          },
        });

        expect(listRes.status).toBe(200);
        expect(Array.isArray(listRes.json)).toBe(true);
        expect(listRes.json.some((fda) => fda.id === fdaId)).toBe(true);

        const listResNormalized = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(listResNormalized.status).toBe(200);
        expect(listResNormalized.json.some((fda) => fda.id === fdaId)).toBe(
          true,
        );

        const dataRes = await httpReq({
          method: 'GET',
          url: buildFdaDataUrl(baseUrl, servicePath, fdaId),
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': multiServicePath,
          },
        });

        expect(dataRes.status).toBe(200);
        expect(Array.isArray(dataRes.json)).toBe(true);
        expect(dataRes.json.length).toBeGreaterThan(0);
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
