// Copyright 2025 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telef�nica Soluciones
// de Inform�tica y Comunicaciones de Espa�a, S.A.U (hereinafter TSOL) and is protected
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

import { normalizeServicePath } from '../lib/utils/fdaScope.js';
import { aggregateFDAUsage } from '../lib/utils/mongo.js';
import { summarizeUsage } from '../lib/utils/usage.js';

const router = express.Router();

/**
 * @swagger
 * /usage:
 *   get:
 *     tags: [Usage]
 *     operationId: getUsage
 *     summary: Retrieve persisted resource accounting
 *     description: >
 *       Returns service totals and either the requested servicePath or a breakdown by servicePath.
 *       Access counters are eventually consistent with in-memory buffers. Refresh totals sum the last
 *       successful refresh of each FDA, not the historical cost of all refreshes. No limits are enforced.
 *     parameters:
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - name: Fiware-ServicePath
 *         in: header
 *         required: false
 *         schema:
 *           type: string
 *         example: /servicepath
 *     responses:
 *       '200':
 *         description: Persisted service and servicePath accounting.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 service:
 *                   $ref: '#/components/schemas/UsageScope'
 *                 servicePath:
 *                   $ref: '#/components/schemas/UsageScope'
 *                 byServicePath:
 *                   type: array
 *                   items:
 *                     allOf:
 *                       - $ref: '#/components/schemas/UsageScope'
 *                       - type: object
 *                         properties:
 *                           servicePath:
 *                             type: string
 *             example:
 *               service:
 *                 fdas: 3
 *                 bytes: 52428800
 *                 objects: 365
 *                 partitions: 365
 *                 queries: 42
 *                 sinceLastFetch: 3
 *                 lastAccessAt: '2026-10-01T09:31:02.000Z'
 *                 refreshDurationMs: 1200
 *                 refreshBytesFetched: 100000000
 *               servicePath:
 *                 fdas: 2
 *                 bytes: 41943040
 *                 objects: 300
 *                 partitions: 300
 *                 queries: 30
 *                 sinceLastFetch: 2
 *                 lastAccessAt: '2026-10-01T09:31:02.000Z'
 *                 refreshDurationMs: 1000
 *                 refreshBytesFetched: 80000000
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 * components:
 *   schemas:
 *     UsageScope:
 *       type: object
 *       properties:
 *         fdas:
 *           type: integer
 *         bytes:
 *           type: integer
 *         objects:
 *           type: integer
 *         partitions:
 *           type: integer
 *         queries:
 *           type: integer
 *         sinceLastFetch:
 *           type: integer
 *         lastAccessAt:
 *           type: string
 *           format: date-time
 *           nullable: true
 *         refreshDurationMs:
 *           type: integer
 *         refreshBytesFetched:
 *           type: integer
 */
router.get('/usage', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');

  if (!service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing Fiware-Service header',
    });
  }
  const groups = await aggregateFDAUsage(service);
  const usage = { service: summarizeUsage(groups) };
  if (servicePath === undefined) {
    usage.byServicePath = groups;
  } else {
    const normalizedPath = normalizeServicePath(servicePath);
    usage.servicePath = summarizeUsage(
      groups.filter((group) => group.servicePath === normalizedPath),
    );
  }
  return res.status(200).json(usage);
});

export default router;
