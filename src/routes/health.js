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

import express from 'express';

import {
  buildHealthPayload,
  buildMetricsText,
  getMetricsContentType,
} from '../lib/metrics.js';

const router = express.Router();

/**
 * @swagger
 * /health:
 *   get:
 *     tags: [Health]
 *     operationId: getHealth
 *     summary: Health check
 *     description: >
 *       Returns the operational status of the service plus runtime and traffic context useful for operations.
 *       Does not require the `Fiware-Service` header.
 *     responses:
 *       '200':
 *         description: Service is running.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/HealthPayload'
 */
router.get('/health', async (req, res) => {
  const payload = await buildHealthPayload();
  res.status(200).json(payload);
});

/**
 * @swagger
 * /metrics:
 *   get:
 *     tags: [Metrics]
 *     operationId: getMetrics
 *     summary: Retrieve telemetry metrics
 *     description: >
 *       Exposes a telemetry endpoint compatible with the Prometheus text format and OpenMetrics content
 *       negotiation.
 *     parameters:
 *       - name: Accept
 *         in: header
 *         required: false
 *         description: >
 *           If it contains `application/openmetrics-text`, the response uses OpenMetrics format; if missing or
 *           it allows `text/plain` (explicitly or through a wildcard value), the response uses Prometheus text format.
 *         schema:
 *           type: string
 *         example: text/plain
 *     responses:
 *       '200':
 *         description: Metrics payload.
 *         content:
 *           text/plain:
 *             schema:
 *               type: string
 *               example: |
 *                 # HELP fda_up Service liveness indicator (1=up).
 *                 # TYPE fda_up gauge
 *                 fda_up 1
 *           application/openmetrics-text:
 *             schema:
 *               type: string
 *       '406':
 *         $ref: '#/components/responses/NotAcceptable'
 */
router.get('/metrics', async (req, res) => {
  const contentNegotiation = getMetricsContentType(req.get('Accept'));

  if (!contentNegotiation.ok) {
    return res.status(406).json({
      error: 'NotAcceptable',
      description:
        'Accept header must allow application/openmetrics-text or text/plain',
    });
  }

  res.setHeader('Content-Type', contentNegotiation.contentType);
  const payload = await buildMetricsText();
  return res.status(200).send(payload);
});

export default router;
