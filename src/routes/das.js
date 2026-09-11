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

import { createDA, getDAs, getDA, putDA, deleteDA } from '../lib/fda.js';
import { validateAllowedFieldsBody } from '../lib/utils/utils.js';
import { validateFdaId } from '../lib/utils/routeHelpers.js';

const router = express.Router();

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das:
 *   get:
 *     tags: [DAs]
 *     operationId: listDas
 *     summary: List DAs
 *     description: Returns all DAs associated with a given FDA.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '200':
 *         description: Array of DAs.
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Da'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`FDANotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const das = await getDAs(service, fdaId, visibility, servicePath);
  return res.status(200).json(das);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das:
 *   post:
 *     tags: [DAs]
 *     operationId: createDa
 *     summary: Create DA
 *     description: >
 *       Creates a new DA on the given FDA. Allowed even while the FDA is still processing its first fetch (an
 *       internal one-row synchronous snapshot is used to validate query compatibility); execution of the DA
 *       itself is blocked until the first fetch completes. Not allowed for only-fresh FDAs (`cached: false`).
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/DaCreateRequest'
 *           example:
 *             id: da_all_alarms
 *             description: Todas las alarmas
 *             query: SELECT * LIMIT 10
 *     responses:
 *       '204':
 *         description: DA created.
 *       '400':
 *         description: '`BadRequest` or `InvalidQueryParam` (a declared param fails its `range`/`enum` restrictions).'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '404':
 *         description: '`FDANotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '409':
 *         description: '`DuplicatedKey`, or `FDAOnlyFresh` when the FDA was created with `cached: false`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/:visibility/fdas/:fdaId/das', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['id', 'query', 'description', 'params']);
  const { id, description, query, params } = body;

  if (!fdaId || !id || !query || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  validateFdaId(id);

  await createDA(
    service,
    fdaId,
    id,
    description,
    query,
    params,
    visibility,
    servicePath,
  );
  return res.sendStatus(204);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das/{daId}:
 *   get:
 *     tags: [DAs]
 *     operationId: getDa
 *     summary: Get DA
 *     description: Returns the requested DA.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/DaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '200':
 *         description: DA object.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Da'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`FDANotFound` or `DaNotFound`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;

  if (!service || !fdaId || !daId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const da = await getDA(service, fdaId, daId, visibility, servicePath);
  return res.status(200).json(da);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das/{daId}:
 *   put:
 *     tags: [DAs]
 *     operationId: updateDa
 *     summary: Update DA
 *     description: Updates an existing DA. `id` must not be included in the body.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/DaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/DaUpdateRequest'
 *           example:
 *             description: Todas las alarmas (actualizado)
 *             query: SELECT * LIMIT 20
 *     responses:
 *       '204':
 *         description: DA updated.
 *       '400':
 *         description: '`BadRequest` or `InvalidQueryParam`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '404':
 *         description: '`FDANotFound` or `DaNotFound`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.put('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['query', 'description', 'params']);
  const { description, query, params } = body;

  if (!service || !fdaId || !daId || !query || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await putDA(
    service,
    fdaId,
    daId,
    description,
    query,
    params,
    visibility,
    servicePath,
  );

  return res.sendStatus(204);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das/{daId}:
 *   delete:
 *     tags: [DAs]
 *     operationId: deleteDa
 *     summary: Delete DA
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/DaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '204':
 *         description: DA deleted.
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`FDANotFound` or `DaNotFound`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.delete('/:visibility/fdas/:fdaId/das/:daId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId, daId } = req.params;

  if (!service || !fdaId || !daId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteDA(service, fdaId, daId, visibility, servicePath);
  return res.sendStatus(204);
});

export default router;
