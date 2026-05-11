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

export function registerVisibilityConstraintsIntegrationTests({
  getBaseUrl,
  service,
  httpReq,
  fdaId,
  daId,
}) {
  describe('Visibility constraints', () => {
    test('GET /{visibility}/... returns 400 for an invalid visibility value', async () => {
      const baseUrl = getBaseUrl();
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/shared/fdas/${fdaId}/das/${daId}/data?minAge=25`,
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidVisibility');
    });

    test('GET /{visibility}/... returns 403 when visibility does not match the FDA visibility', async () => {
      const baseUrl = getBaseUrl();
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/private/fdas/${fdaId}/das/${daId}/data?minAge=25`,
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(403);
      expect(res.json.error).toBe('VisibilityMismatch');
    });
  });
}
