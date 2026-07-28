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
import { MongoClient } from 'mongodb';

export function registerFdaLifecycleIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  fdaId,
  fdaId2,
  fdaId3,
  httpReq,
  waitUntilFDACompleted,
  getMongoUri,
}) {
  test('GET /fdas/:fdaId returns expected FDA', async () => {
    const baseUrl = getBaseUrl();
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status >= 400) {
      console.error(
        'GET /fdas/:fdaId failed:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(200);
    expect(Object.keys(res.json).length).toBeGreaterThan(0);
    expect(res.json.id).toBeUndefined();
    expect(res.json.fdaId).toBeUndefined();
    expect(res.json.service).toBeUndefined();
    expect(res.json.visibility).toBeUndefined();
    expect(res.json.servicePath).toBeUndefined();
  });

  test('PUT /fdas/:fdaID reuploads FDA', async () => {
    const baseUrl = getBaseUrl();

    const beforeRefresh = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(beforeRefresh.status).toBe(200);
    expect(typeof beforeRefresh.json.initFetch).toBe('string');
    expect(typeof beforeRefresh.json.lastFetch).toBe('string');

    const beforeInitFetchMs = Date.parse(beforeRefresh.json.initFetch);
    const beforeLastFetchMs = Date.parse(beforeRefresh.json.lastFetch);

    expect(Number.isNaN(beforeInitFetchMs)).toBe(false);
    expect(Number.isNaN(beforeLastFetchMs)).toBe(false);
    expect(beforeLastFetchMs).toBeGreaterThanOrEqual(beforeInitFetchMs);

    const res = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status >= 400) {
      console.error(
        'PUT /fdas/:fdaId failed:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId });

    const afterRefresh = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(afterRefresh.status).toBe(200);
    expect(typeof afterRefresh.json.initFetch).toBe('string');
    expect(typeof afterRefresh.json.lastFetch).toBe('string');

    const afterInitFetchMs = Date.parse(afterRefresh.json.initFetch);
    const afterLastFetchMs = Date.parse(afterRefresh.json.lastFetch);

    expect(Number.isNaN(afterInitFetchMs)).toBe(false);
    expect(Number.isNaN(afterLastFetchMs)).toBe(false);
    expect(afterLastFetchMs).toBeGreaterThanOrEqual(afterInitFetchMs);
    expect(afterInitFetchMs).toBeGreaterThanOrEqual(beforeLastFetchMs);
  });

  test('PUT /fdas/:fdaId triggers AlreadyFetching if concurrent', async () => {
    const baseUrl = getBaseUrl();
    httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
      headers: { 'Fiware-Service': service },
    });

    const put2 = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
      headers: { 'Fiware-Service': service },
    });

    expect(put2.status).toBe(409);
    expect(put2.json.error).toBe('AlreadyFetching');

    await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
  });

  test('PUT /fdas/:fdaId throws InvalidState if FDA in unexpected status', async () => {
    const baseUrl = getBaseUrl();
    const client = new MongoClient(getMongoUri());
    await client.connect();
    const collection = client.db().collection('fdas');

    await collection.updateOne(
      { fdaId: fdaId3, service },
      { $set: { status: 'transforming' } },
    );

    const res = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
      headers: { 'Fiware-Service': service },
    });

    expect(res.status).toBe(409);
    expect(res.json.error).toBe('InvalidState');

    await collection.updateOne(
      { fdaId: fdaId3, service },
      { $set: { status: 'completed' } },
    );
    await client.close();
  });

  test('DELETE /fdas/:fdaId removes given FDA', async () => {
    const baseUrl = getBaseUrl();
    const deleteFDA = await httpReq({
      method: 'DELETE',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    if (deleteFDA.status >= 400) {
      console.error(
        'DELETE /fdas/:fdaId failed:',
        deleteFDA.status,
        deleteFDA.json ?? deleteFDA.text,
      );
    }
    expect(deleteFDA.status).toBe(204);

    const getFDA = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(getFDA.status).toBe(404);
  });

  test('MongoDB integration: POST /fdas + get /fdas/:fdaId', async () => {
    const baseUrl = getBaseUrl();
    const postFDA = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId2,
        query: 'SELECT id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'interval',
          params: {
            refreshInterval: '1 hour',
          },
        },
      },
    });

    if (postFDA.status >= 400) {
      console.error(
        'POST /fdas failed:',
        postFDA.status,
        postFDA.json ?? postFDA.text,
      );
    }
    expect(postFDA.status).toBe(202);

    const completedFDA = await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: fdaId2,
    });

    expect(completedFDA).toMatchObject({
      query: 'SELECT id, name, age FROM public.users ORDER BY id',
      description: 'users dataset',
      status: 'completed',
      progress: 100,
      refreshPolicy: {
        type: 'interval',
        params: {
          refreshInterval: '1 hour',
        },
      },
    });
    expect(completedFDA.fdaId).toBeUndefined();
    expect(completedFDA.service).toBeUndefined();
    expect(completedFDA.servicePath).toBeUndefined();
    expect(completedFDA.initFetch).toBeDefined();
    expect(typeof completedFDA.initFetch).toBe('string');
    expect(completedFDA.lastFetch).toBeDefined();
    expect(typeof completedFDA.lastFetch).toBe('string');

    const initFetchMs = Date.parse(completedFDA.initFetch);
    const lastFetchMs = Date.parse(completedFDA.lastFetch);
    expect(Number.isNaN(initFetchMs)).toBe(false);
    expect(Number.isNaN(lastFetchMs)).toBe(false);
    expect(lastFetchMs).toBeGreaterThanOrEqual(initFetchMs);
  });
}
