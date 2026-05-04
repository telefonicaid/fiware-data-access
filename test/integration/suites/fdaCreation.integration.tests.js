import { test, expect } from '@jest/globals';

export function registerFdaCreationIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  fdaId,
  fdaId3,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  test('POST /fdas creates an FDA (uploads CSV then converts to Parquet)', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId,
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
      },
    });

    if (res.status >= 400) {
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId });
  });

  test('POST /fdas tries to creates an FDA without id and is detected', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: null,
        // query base to extract from PG to CSV
        query: 'SELECT id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
      },
    });

    if (res.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(400);
  });

  test('POST /fdas try creates an FDA without body', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
    });
    if (res.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(400);
  });

  test('POST /fdas with duplicate id returns error', async () => {
    const baseUrl = getBaseUrl();

    await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: { 'Fiware-Service': service },
      body: {
        id: fdaId3,
        query: 'SELECT id FROM public.users',
        description: 'duplicate test',
      },
    });

    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId3, // same id
        query: 'SELECT id FROM public.users',
        description: 'duplicate test',
      },
    });

    expect(res.status).toBe(409);
    expect(res.json.error).toBe('DuplicatedKey');

    await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
  });

  test('POST /fdas allows same id in same service when servicePath differs', async () => {
    const baseUrl = getBaseUrl();
    const scopedFdaId = 'fda_same_id_diff_servicepath';
    const otherServicePath = '/other-path';

    const firstCreate = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: scopedFdaId,
        query: 'SELECT id FROM public.users',
        description: 'scope one',
      },
    });

    expect(firstCreate.status).toBe(202);

    const secondCreate = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': otherServicePath,
      },
      body: {
        id: scopedFdaId,
        query: 'SELECT id FROM public.users',
        description: 'scope two',
      },
    });

    expect(secondCreate.status).toBe(202);

    const wrongScopeRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': '/unknown-path',
      },
    });

    expect(wrongScopeRead.status).toBe(403);
    expect(wrongScopeRead.json.error).toBe('ServicePathMismatch');

    const scopeOneRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
    });

    expect(scopeOneRead.status).toBe(200);

    const scopeTwoRead = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': otherServicePath,
      },
    });

    expect(scopeTwoRead.status).toBe(200);

    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: scopedFdaId,
      servicePath,
    });
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: scopedFdaId,
      servicePath: otherServicePath,
    });
  });

  test('POST /fdas pending allows DA creation but rejects GET /{visibility}/fdas/{fdaId}/das/{daId}/data until first completion', async () => {
    const baseUrl = getBaseUrl();
    const pendingFdaId = 'fda_pending_first_fetch';
    const pendingDaId = 'da_pending_first_fetch';

    const createFda = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: pendingFdaId,
        // Force a long first fetch to keep the FDA non-queryable for this test.
        query:
          'SELECT id, name, age FROM public.users, (SELECT pg_sleep(6)) AS delayed_fetch',
        description: 'pending fda test',
      },
    });

    expect(createFda.status).toBe(202);

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas/${pendingFdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: pendingDaId,
        description: 'pending da test',
        query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
        params: [
          {
            name: 'minAge',
            type: 'Number',
            required: true,
          },
        ],
      },
    });

    expect(createDa.status).toBe(201);

    const queryRes = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
        minAge: 20,
      }),
      headers: { 'Fiware-Service': service },
    });

    if (queryRes.status >= 400) {
      console.error(
        'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected while FDA is pending:',
        queryRes.status,
        queryRes.json ?? queryRes.text,
      );
    }

    expect(queryRes.status).toBe(409);
    expect(queryRes.json.error).toBe('FDAUnavailable');

    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: pendingFdaId,
      timeout: 30000,
    });

    const queryAfterCompletion = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
        minAge: 20,
      }),
      headers: { 'Fiware-Service': service },
    });

    expect(queryAfterCompletion.status).toBe(200);
    expect(queryAfterCompletion.json).toEqual([
      { id: '1', name: 'ana', age: '30' },
      { id: '3', name: 'carlos', age: '40' },
    ]);
  });

  test('GET /fdas returns list', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status >= 400) {
      console.error('GET /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(200);
    expect(Array.isArray(res.json)).toBe(true);
    expect(res.json.some((x) => x.id === fdaId)).toBe(true);
  });
}
