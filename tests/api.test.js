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

import request from 'supertest';

const API_URL = 'http://localhost:8081';

describe('API Integrations', () => {
  it('POST /fetchSet should return 201', async () => {
    const body = {
      setId: 'set1',
      database: 'testdb',
      table: 'mytable',
      bucket: 'bucket-test',
      path: 'path-test',
    };

    const res = await request(API_URL)
      .post('/fetchSet?service=test')
      .send(body);

    expect(res.status).toBe(201);
    expect(res.body.message).toBe('Set fetched correctly');
  });

  it('POST /sets/:setId/fdas should create FDA', async () => {
    const res = await request(API_URL).post('/sets/set1/fdas').send({
      id: 'fda1',
      description: 'test desc',
      query: 'SELECT 1',
    });

    expect(res.status).toBe(201);
  });

  it('GET /querySet should return query results', async () => {
    const res = await request(API_URL)
      .get('/querySet')
      .query({ setId: 'set1', id: 'fda1' });

    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
  });
});
