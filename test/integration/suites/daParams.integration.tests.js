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

import { test, expect } from '@jest/globals';

export function registerDaParamsIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  fdaId,
  daId2,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  test('PUT /fdas/:fdaId/das/:daId updates DA with incorrect params', async () => {
    const baseUrl = getBaseUrl();
    const daIdToUpdate = 'da_update';

    const rangeUpdateRes = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        id: 'ignored_in_put',
        description: 'updated filter',
        params: [
          {
            name: 'activity',
            type: 'Number',
            range: ['badRange', 14],
          },
        ],
      },
    });

    if (rangeUpdateRes.status >= 400) {
      console.error(
        'PUT /das failed as expected:',
        rangeUpdateRes.status,
        rangeUpdateRes.json ?? rangeUpdateRes.text,
      );
    }

    expect(rangeUpdateRes.status).toBe(400);

    const enumUpdateRes = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        id: 'ignored_in_put',
        description: 'updated filter',
        params: [
          {
            name: 'activity',
            type: 'Number',
            enum: ['option', true],
          },
        ],
      },
    });

    if (enumUpdateRes.status >= 400) {
      console.error(
        'PUT /das failed as expected:',
        enumUpdateRes.status,
        enumUpdateRes.json ?? enumUpdateRes.text,
      );
    }

    expect(enumUpdateRes.status).toBe(400);
  });

  test('POST /fdas/:fdaId/das rejects incompatible default values', async () => {
    const baseUrl = getBaseUrl();
    const fdaBadDefaultId = 'fda_bad_default';

    const createFda = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaBadDefaultId,
        description: 'invalid default test fda',
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
      },
    });

    expect(createFda.status).toBe(202);
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: fdaBadDefaultId,
    });

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaBadDefaultId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: 'da_bad_default',
        description: 'should reject invalid default',
        query: `
          SELECT id, name
          WHERE authorized = $authorized
          ORDER BY id
        `,
        params: [
          {
            name: 'authorized',
            type: 'Boolean',
            default: 'notBool',
          },
        ],
      },
    });

    expect(createDa.status).toBe(400);
    expect(createDa.json.error).toBe('InvalidParam');
    expect(createDa.json.description).toContain(
      'Default value for param "authorized" not of valid type (Boolean).',
    );
  });

  test('POST /fdas/:fdaId/das rejects empty body', async () => {
    const baseUrl = getBaseUrl();
    const fdaBadDefaultId = 'fda_bad_body';

    const createFda = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaBadDefaultId,
        description: 'invalid default test fda',
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
      },
    });

    expect(createFda.status).toBe(202);
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: fdaBadDefaultId,
    });

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaBadDefaultId}/das`,
      headers: { 'Fiware-Service': service },
    });

    expect(createDa.status).toBe(400);
  });

  test('POST /fdas/:fdaId/das + GET /{visibility}/fdas/{fdaId}/das/{daId}/data using default params', async () => {
    const baseUrl = getBaseUrl();

    const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge AND name = $name AND timeinstant = $timeinstant AND authorized = $authorized
      ORDER BY id;
    `;

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: daId2,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'name',
            type: 'Text',
            required: true,
            enum: ['ana', 'carlos'],
          },
          {
            name: 'minAge',
            type: 'Number',
            default: 25,
            range: [20, 50],
          },
          {
            name: 'timeinstant',
            type: 'DateTime',
            default: '2020-08-17T18:25:28.332Z',
          },
          {
            name: 'authorized',
            type: 'Boolean',
            default: true,
          },
        ],
      },
    });

    if (createDa.status >= 400) {
      console.error(
        'POST /das failed:',
        createDa.status,
        createDa.json ?? createDa.text,
      );
    }
    expect(createDa.status).toBe(201);

    const noTypeDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_noType`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'minAge',
            default: 25,
            range: ['badRange', 50],
          },
        ],
      },
    });

    if (noTypeDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        noTypeDa.status,
        noTypeDa.json ?? noTypeDa.text,
      );
    }
    expect(noTypeDa.status).toBe(400);

    const badTypeDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badType`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'minAge',
            type: 'FakeType',
            default: 25,
            range: ['badRange', 50],
          },
        ],
      },
    });

    if (badTypeDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        badTypeDa.status,
        badTypeDa.json ?? badTypeDa.text,
      );
    }
    expect(badTypeDa.status).toBe(400);

    const badDefaultDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badDefault`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'authorized',
            type: 'Boolean',
            default: 'notBool',
          },
        ],
      },
    });

    if (badDefaultDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        badDefaultDa.status,
        badDefaultDa.json ?? badDefaultDa.text,
      );
    }
    expect(badDefaultDa.status).toBe(400);

    const badEnumDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badType`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'name',
            type: 'Text',
            default: 'carlos',
            enum: [true, false],
          },
        ],
      },
    });

    if (badEnumDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        badEnumDa.status,
        badEnumDa.json ?? badEnumDa.text,
      );
    }
    expect(badEnumDa.status).toBe(400);

    const createBadDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badRange`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'minAge',
            type: 'Number',
            default: 25,
            range: ['badRange', 50],
          },
        ],
      },
    });

    if (createBadDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        createBadDa.status,
        createBadDa.json ?? createBadDa.text,
      );
    }
    expect(createBadDa.status).toBe(400);

    const createBadRangeDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badRangeOrder`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'minAge',
            type: 'Number',
            default: 25,
            range: [60, 50],
          },
        ],
      },
    });

    if (createBadRangeDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        createBadRangeDa.status,
        createBadRangeDa.json ?? createBadRangeDa.text,
      );
    }
    expect(createBadRangeDa.status).toBe(400);

    const badRangeLengthDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: `${daId2}_badRangeLength`,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'minAge',
            type: 'Number',
            default: 25,
            range: [40, 50, 60],
          },
        ],
      },
    });

    if (badRangeLengthDa.status >= 400) {
      console.error(
        'POST /das failed as expected:',
        badRangeLengthDa.status,
        badRangeLengthDa.json ?? badRangeLengthDa.text,
      );
    }
    expect(badRangeLengthDa.status).toBe(400);

    const enumQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        minAge: 25,
        name: 'fakeNAme',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (enumQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        enumQueryRes.status,
        enumQueryRes.json ?? enumQueryRes.text,
      );
    }
    expect(enumQueryRes.status).toBe(400);

    const rangeQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        minAge: 60,
        name: 'ana',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (rangeQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        rangeQueryRes.status,
        rangeQueryRes.json ?? rangeQueryRes.text,
      );
    }
    expect(rangeQueryRes.status).toBe(400);

    const defaultsQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        name: 'carlos',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (defaultsQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed:',
        defaultsQueryRes.status,
        defaultsQueryRes.json ?? defaultsQueryRes.text,
      );
    }
    expect(defaultsQueryRes.status).toBe(200);
    expect(defaultsQueryRes.json).toEqual([
      { id: '3', name: 'carlos', age: '40' },
    ]);

    const requiredQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        minAge: 25,
      }),
      headers: { 'Fiware-Service': service },
    });

    if (requiredQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        requiredQueryRes.status,
        requiredQueryRes.json ?? requiredQueryRes.text,
      );
    }
    expect(requiredQueryRes.status).toBe(400);

    const typeQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        minAge: 'text',
        name: 'carlos',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (typeQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        typeQueryRes.status,
        typeQueryRes.json ?? typeQueryRes.text,
      );
    }
    expect(typeQueryRes.status).toBe(400);

    const dateQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        name: 'carlos',
        timeinstant: '2020-08-17 18:25:28.332+01:00',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (dateQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        dateQueryRes.status,
        dateQueryRes.json ?? dateQueryRes.text,
      );
    }
    expect(dateQueryRes.status).toBe(400);

    const boolQueryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        name: 'carlos',
        authorized: 'notBool',
      }),
      headers: { 'Fiware-Service': service },
    });

    if (boolQueryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
        boolQueryRes.status,
        boolQueryRes.json ?? boolQueryRes.text,
      );
    }
    expect(boolQueryRes.status).toBe(400);

    const numberCoercionRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        minAge: 30,
        name: 'carlos',
      }),
      headers: { 'Fiware-Service': service },
    });

    expect(numberCoercionRes.status).toBe(200);
    expect(numberCoercionRes.json).toEqual([
      { id: '3', name: 'carlos', age: '40' },
    ]);

    const booleanCoercionRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        name: 'ana',
        authorized: true,
      }),
      headers: { 'Fiware-Service': service },
    });

    expect(booleanCoercionRes.status).toBe(200);
    expect(booleanCoercionRes.json).toEqual([
      { id: '1', name: 'ana', age: '30' },
    ]);

    const dateTimeCoercionRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
        name: 'carlos',
        timeinstant: '2020-08-17T18:25:28.332Z',
      }),
      headers: { 'Fiware-Service': service },
    });

    expect(dateTimeCoercionRes.status).toBe(200);
    expect(dateTimeCoercionRes.json).toEqual([
      { id: '3', name: 'carlos', age: '40' },
    ]);
  });
}
