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

export function registerDatasourcesIntegrationTests({
  getBaseUrl,
  service,
  getPgHost,
  getPgPort,
  httpReq,
}) {
  test('POST /datasources provisions default datasource required by FDA tests', async () => {
    const baseUrl = getBaseUrl();

    const deleteExisting = await httpReq({
      method: 'DELETE',
      url: `${baseUrl}/datasources/default`,
      headers: { 'Fiware-Service': service },
    });

    expect([204, 404]).toContain(deleteExisting.status);

    const createRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId: 'default',
        type: 'postgres',
        config: {
          user: 'postgres',
          password: 'postgres',
          host: getPgHost(),
          port: getPgPort(),
          database: service,
        },
      },
    });

    if (createRes.status >= 400) {
      console.error(
        'POST /datasources default failed:',
        createRes.status,
        createRes.json ?? createRes.text,
      );
    }

    expect(createRes.status).toBe(200);

    const getRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/datasources/default`,
      headers: { 'Fiware-Service': service },
    });

    expect(getRes.status).toBe(200);
    expect(getRes.json).toMatchObject({
      datasourceId: 'default',
      type: 'postgres',
      config: {
        user: 'postgres',
        password: 'postgres',
        host: getPgHost(),
        port: getPgPort(),
        database: service,
      },
    });
  });

  test('POST /datasources rejects duplicate datasourceId in same service', async () => {
    const baseUrl = getBaseUrl();

    const duplicateRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId: 'default',
        type: 'postgres',
        config: {
          user: 'postgres',
          password: 'postgres',
          host: getPgHost(),
          port: getPgPort(),
          database: service,
        },
      },
    });

    expect(duplicateRes.status).toBe(409);
    expect(duplicateRes.json.error).toBe('DuplicatedKey');
  });

  test('GET/PUT/DELETE /datasources/{datasourceId} manages a secondary datasource', async () => {
    const baseUrl = getBaseUrl();
    const datasourceId = 'analytics';

    const createRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/datasources`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        datasourceId,
        type: 'postgres',
        config: {
          user: 'postgres',
          password: 'postgres',
          host: getPgHost(),
          port: getPgPort(),
          database: service,
        },
      },
    });

    expect(createRes.status).toBe(200);

    const listRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/datasources`,
      headers: { 'Fiware-Service': service },
    });

    expect(listRes.status).toBe(200);
    expect(listRes.json.some((x) => x.datasourceId === datasourceId)).toBe(
      true,
    );

    const updateRes = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/datasources/${datasourceId}`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        type: 'postgres',
        config: {
          user: 'postgres',
          password: 'postgres',
          host: getPgHost(),
          port: getPgPort(),
          database: service,
        },
      },
    });

    expect(updateRes.status).toBe(204);

    const deleteRes = await httpReq({
      method: 'DELETE',
      url: `${baseUrl}/datasources/${datasourceId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(deleteRes.status).toBe(204);

    const missingRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/datasources/${datasourceId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(missingRes.status).toBe(404);
    expect(missingRes.json.error).toBe('DatasourceNotFound');
  });
}
