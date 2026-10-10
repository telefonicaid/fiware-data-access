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

import { getUsage } from '../lib/quotas.js';
import { normalizeServicePath } from '../lib/utils/fdaScope.js';

const router = express.Router();

/**
 * @swagger
 * /usage:
 *   get:
 *     tags: [Usage]
 *     operationId: getUsage
 *     summary: Retrieve resource usage and quotas
 *     description: >
 *       Returns the storage and FDA count consumed by the provided `Fiware-Service`, the limits that apply and
 *       the remaining room. When `Fiware-ServicePath` is sent the response also includes that servicePath scope;
 *       otherwise it lists the usage of every servicePath in the service. A `null` limit means unlimited.
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
 *         description: Usage, limits and available room.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 service:
 *                   $ref: '#/components/schemas/UsageScope'
 *                 servicePath:
 *                   $ref: '#/components/schemas/UsageScope'
 *                 fda:
 *                   type: object
 *                   properties:
 *                     limits:
 *                       type: object
 *                       properties:
 *                         maxBytes:
 *                           type: integer
 *                           nullable: true
 *                         maxFetchBytes:
 *                           type: integer
 *                           nullable: true
 *                 byServicePath:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       servicePath:
 *                         type: string
 *                       fdas:
 *                         type: integer
 *                       bytes:
 *                         type: integer
 *             example:
 *               service:
 *                 limits: { maxFDAs: 20, maxBytes: 10737418240 }
 *                 used: { fdas: 3, bytes: 52428800 }
 *                 available: { fdas: 17, bytes: 10684989440 }
 *               servicePath:
 *                 limits: { maxFDAs: null, maxBytes: null }
 *                 used: { fdas: 2, bytes: 41943040 }
 *                 available: { fdas: null, bytes: null }
 *               fda:
 *                 limits: { maxBytes: 1073741824, maxFetchBytes: null }
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 * components:
 *   schemas:
 *     UsageScope:
 *       type: object
 *       properties:
 *         limits:
 *           type: object
 *           properties:
 *             maxFDAs:
 *               type: integer
 *               nullable: true
 *             maxBytes:
 *               type: integer
 *               nullable: true
 *         used:
 *           type: object
 *           properties:
 *             fdas:
 *               type: integer
 *             bytes:
 *               type: integer
 *         available:
 *           type: object
 *           properties:
 *             fdas:
 *               type: integer
 *               nullable: true
 *             bytes:
 *               type: integer
 *               nullable: true
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

  const usage = await getUsage(
    service,
    servicePath === undefined ? undefined : normalizeServicePath(servicePath),
  );
  return res.status(200).json(usage);
});

export default router;
