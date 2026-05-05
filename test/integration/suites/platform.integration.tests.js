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

export function registerPlatformIntegrationTests({ getAppPort, httpReq }) {
  test('GET /health returns UP status', async () => {
    const appPort = getAppPort();
    const res = await httpReq({
      method: 'GET',
      url: `http://127.0.0.1:${appPort}/health`,
    });

    expect(res.status).toBe(200);
    expect(res.json.status).toBe('UP');
    expect(typeof res.json.uptimeSeconds).toBe('number');
  });

  test('GET /metrics returns text-format telemetry', async () => {
    const appPort = getAppPort();
    const res = await httpReq({
      method: 'GET',
      url: `http://127.0.0.1:${appPort}/metrics`,
    });

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.headers['content-type']).toContain('version=0.0.4');
    expect(res.headers['content-type']).toContain('charset=utf-8');
    expect(res.text).toContain('# TYPE fda_up gauge');
    expect(res.text).toContain('# EOF');
  });
}
