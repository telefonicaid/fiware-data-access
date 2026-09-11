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
  createDatasourceForService,
  getDatasourcesForService,
  getDatasourceForService,
  updateDatasourceForService,
  deleteDatasourceForService,
} from '../lib/fda.js';
import { validateAllowedFieldsBody } from '../lib/utils/utils.js';
import { validateFdaId } from '../lib/utils/routeHelpers.js';

const router = express.Router();

/**
 * @swagger
 * /datasources:
 *   post:
 *     tags: [Datasources]
 *     operationId: createDatasource
 *     summary: Create datasource
 *     description: >
 *       Creates a datasource for the provided `Fiware-Service`. Creation validates the connection before
 *       persisting: for `postgres` by opening a PostgreSQL connection, for `mongodb` by opening a MongoDB
 *       connection.
 *     parameters:
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/DatasourceCreateRequest'
 *           examples:
 *             postgres:
 *               summary: PostgreSQL datasource
 *               value:
 *                 type: postgres
 *                 config:
 *                   username: postgres
 *                   password: postgres
 *                   host: localhost
 *                   port: 5432
 *                   database: trantor
 *             mongodb:
 *               summary: MongoDB datasource
 *               value:
 *                 datasourceId: mongo-default
 *                 type: mongodb
 *                 config:
 *                   uri: 'mongodb://localhost:27017'
 *                   database: trantor
 *     responses:
 *       '204':
 *         description: Datasource created.
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '409':
 *         description: '`DuplicatedKey`: a datasource with this id already exists for the service.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/datasources', async (req, res) => {
  const service = req.get('Fiware-Service');
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['datasourceId', 'type', 'config']);
  const { type, config: dsConfig } = body;
  let { datasourceId } = body;

  if (!service || !type || !dsConfig) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing required fields: datasourceId, type, config',
    });
  }

  if (!datasourceId) {
    datasourceId = 'default';
  }

  validateFdaId(datasourceId);

  await createDatasourceForService(service, datasourceId, type, dsConfig);
  return res.sendStatus(204);
});

/**
 * @swagger
 * /datasources:
 *   get:
 *     tags: [Datasources]
 *     operationId: listDatasources
 *     summary: List datasources
 *     description: Returns all datasources for the provided `Fiware-Service`.
 *     parameters:
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *     responses:
 *       '200':
 *         description: Array of datasource objects.
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Datasource'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 */
router.get('/datasources', async (req, res) => {
  const service = req.get('Fiware-Service');

  if (!service) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing Fiware-Service header',
    });
  }

  const datasources = await getDatasourcesForService(service);
  return res.status(200).json(datasources);
});

/**
 * @swagger
 * /datasources/{datasourceId}:
 *   get:
 *     tags: [Datasources]
 *     operationId: getDatasource
 *     summary: Get datasource
 *     description: Returns one datasource from the provided `Fiware-Service`.
 *     parameters:
 *       - $ref: '#/components/parameters/DatasourceIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *     responses:
 *       '200':
 *         description: Datasource object.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Datasource'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`DatasourceNotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/datasources/:datasourceId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { datasourceId } = req.params;

  if (!service || !datasourceId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const datasource = await getDatasourceForService(service, datasourceId);
  return res.status(200).json(datasource);
});

/**
 * @swagger
 * /datasources/{datasourceId}:
 *   put:
 *     tags: [Datasources]
 *     operationId: updateDatasource
 *     summary: Update datasource
 *     description: >
 *       Updates one datasource from the provided `Fiware-Service`. When present, the resulting configuration is
 *       validated (by opening a connection) before the update is stored.
 *     parameters:
 *       - $ref: '#/components/parameters/DatasourceIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/DatasourceUpdateRequest'
 *     responses:
 *       '204':
 *         description: Datasource updated.
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`DatasourceNotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.put('/datasources/:datasourceId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { datasourceId } = req.params;
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, ['type', 'config']);
  const { type, config: dsConfig } = body;

  if (!service || !datasourceId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await updateDatasourceForService(service, datasourceId, type, dsConfig);
  return res.sendStatus(204);
});

/**
 * @swagger
 * /datasources/{datasourceId}:
 *   delete:
 *     tags: [Datasources]
 *     operationId: deleteDatasource
 *     summary: Delete datasource
 *     description: >
 *       Deletes one datasource from the provided `Fiware-Service`. Rejected with `409 Conflict` while any FDA in
 *       that service still uses the datasource (for datasource `default`, legacy FDAs without an explicit
 *       `datasourceId` also count as users). Does not validate whether existing FDAs reference the datasource
 *       beyond that check; any dependent FDA operation will fail later if datasource resolution is required.
 *     parameters:
 *       - $ref: '#/components/parameters/DatasourceIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *     responses:
 *       '204':
 *         description: Datasource deleted.
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`DatasourceNotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '409':
 *         description: '`DatasourceInUse`: the datasource is still referenced by one or more FDAs.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.delete('/datasources/:datasourceId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const { datasourceId } = req.params;

  if (!service || !datasourceId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteDatasourceForService(service, datasourceId);
  return res.sendStatus(204);
});

export default router;
