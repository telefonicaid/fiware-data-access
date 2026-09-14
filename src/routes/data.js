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
  executeQuery,
  executeFDAQuery,
  executeQueryStream,
  executeFDAQueryStream,
} from '../lib/fda.js';
import { validateForbiddenFieldsQuery } from '../lib/utils/utils.js';
import { toCdaJson } from '../lib/utils/outputFormat.js';
import {
  resolveServiceContextFromRequest,
  getOutputTypeFromAcceptHeader,
  getQueryStyleOutputType,
  splitDaQueryStyleParams,
  sendRowsByOutputType,
} from '../lib/utils/routeHelpers.js';

const router = express.Router();

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/das/{daId}/data:
 *   get:
 *     tags: [Data]
 *     operationId: queryDaData
 *     summary: Run a Data Access (DA) query
 *     description: >
 *       Executes the stored parameterized query for the given DA against the FDA's Parquet snapshot. Query
 *       execution is blocked until the FDA's first successful fetch completes (`409 FDAUnavailable`); after
 *       that, execution is allowed even while a later regeneration is in progress (the last available snapshot
 *       is used).
 *
 *
 *       Supports two mutually exclusive request styles, selected by which parameters are present: **header-style**
 *       (`Fiware-Service`/`Fiware-ServicePath` headers, response format chosen via `Accept`) or **query-style**
 *       (`service`/`servicePath` query parameters, response format chosen via `outputType`). Mixing both styles
 *       in the same request returns `409 RequestStyleConflict`. Any parameter declared in the DA's `params`
 *       array can additionally be supplied as a query parameter regardless of style (e.g. `?pattern=%25foo%25`);
 *       the `fresh` query field is always rejected.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/DaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeaderOptional'
 *       - $ref: '#/components/parameters/FiwareServicePathHeaderOptional'
 *       - $ref: '#/components/parameters/AcceptDataHeader'
 *       - $ref: '#/components/parameters/ServiceQueryOptional'
 *       - $ref: '#/components/parameters/ServicePathQueryOptional'
 *       - $ref: '#/components/parameters/OutputTypeQueryOptional'
 *     responses:
 *       '200':
 *         $ref: '#/components/responses/DataQueryResult'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '403':
 *         description: '`VisibilityMismatch`'
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
 *       '406':
 *         $ref: '#/components/responses/NotAcceptable'
 *       '409':
 *         description: '`FDAUnavailable` (first fetch not completed) or `RequestStyleConflict` (mixed request styles).'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/:visibility/fdas/:fdaId/das/:daId/data', async (req, res) => {
  const { visibility, fdaId, daId } = req.params;
  const { service, servicePath, style } = resolveServiceContextFromRequest(req);

  let outputType;
  let queryParams;

  if (style === 'query') {
    validateForbiddenFieldsQuery(req.query, ['fresh']);
    outputType = getQueryStyleOutputType(req.query);
    queryParams = splitDaQueryStyleParams(req.query);
  } else {
    validateForbiddenFieldsQuery(req.query, ['outputType', 'fresh']);
    outputType = getOutputTypeFromAcceptHeader(req);
    queryParams = { ...req.query };
  }

  if (!fdaId || !daId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const params = {
    fdaId,
    daId,
    ...queryParams,
  };

  if (outputType === 'ndjson' || outputType === 'csv') {
    return executeQueryStream({
      service,
      visibility,
      servicePath,
      params,
      req,
      res,
      format: outputType,
    });
  }

  const rows = await executeQuery({
    service,
    visibility,
    servicePath,
    params,
  });

  const responseRows =
    outputType === 'cda' ? toCdaJson(rows, queryParams) : rows;

  return sendRowsByOutputType(res, responseRows, outputType);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}/data:
 *   get:
 *     tags: [Data]
 *     operationId: queryFdaData
 *     summary: Run the FDA base query directly (always fresh)
 *     description: >
 *       Runs the FDA base query directly against the source datasource. Always fresh; does not use the Parquet
 *       cache. Requires the API instance to run with `FDA_ROLE_SYNCQUERIES=true`, and is subject to the
 *       `FDA_MAX_CONCURRENT_FRESH_QUERIES` limit.
 *
 *
 *       Supports the same two mutually exclusive request styles as the DA data endpoint (header-style via
 *       `Fiware-Service`/`Fiware-ServicePath`/`Accept`, or query-style via `service`/`servicePath`/`outputType`),
 *       but unlike the DA endpoint **no additional query parameters are allowed** in either style; any extra
 *       query parameter (including `fresh`) is rejected with `400 BadRequest`.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeaderOptional'
 *       - $ref: '#/components/parameters/FiwareServicePathHeaderOptional'
 *       - $ref: '#/components/parameters/AcceptDataHeader'
 *       - $ref: '#/components/parameters/ServiceQueryOptional'
 *       - $ref: '#/components/parameters/ServicePathQueryOptional'
 *       - $ref: '#/components/parameters/OutputTypeQueryOptional'
 *     responses:
 *       '200':
 *         $ref: '#/components/responses/DataQueryResult'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '403':
 *         description: '`VisibilityMismatch`'
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
 *       '406':
 *         $ref: '#/components/responses/NotAcceptable'
 *       '409':
 *         description: '`FDANotOnlyFresh`: the FDA is cached (`cached: true`); query it through a DA instead.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '429':
 *         description: '`TooManyFreshQueries`: `FDA_MAX_CONCURRENT_FRESH_QUERIES` exceeded.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '503':
 *         description: '`SyncQueriesDisabled`: the API instance runs with `FDA_ROLE_SYNCQUERIES=false`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/:visibility/fdas/:fdaId/data', async (req, res) => {
  const { visibility, fdaId } = req.params;
  const { service, servicePath, style } = resolveServiceContextFromRequest(req);

  let outputType;
  if (style === 'query') {
    validateForbiddenFieldsQuery(req.query, ['fresh']);
    const rest = { ...req.query };
    delete rest.service;
    delete rest.servicePath;
    delete rest.outputType;
    if (Object.keys(rest).length > 0) {
      return res.status(400).json({
        error: 'BadRequest',
        description: 'FDA fresh query does not accept query parameters',
      });
    }

    outputType = getQueryStyleOutputType(req.query);
  } else {
    if (Object.keys(req.query).length > 0) {
      return res.status(400).json({
        error: 'BadRequest',
        description: 'FDA fresh query does not accept query parameters',
      });
    }

    outputType = getOutputTypeFromAcceptHeader(req);
  }

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  if (outputType === 'ndjson' || outputType === 'csv') {
    return executeFDAQueryStream({
      service,
      visibility,
      servicePath,
      fdaId,
      req,
      res,
      format: outputType,
    });
  }

  const rows = await executeFDAQuery({
    service,
    visibility,
    servicePath,
    fdaId,
  });

  const responseRows = outputType === 'cda' ? toCdaJson(rows) : rows;

  return sendRowsByOutputType(res, responseRows, outputType);
});

export default router;
