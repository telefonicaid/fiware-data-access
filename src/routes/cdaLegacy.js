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

import { handleCdaQuery } from '../lib/compat/cdaAdapter.js';
import {
  VALID_OUTPUT_TYPES,
  LEGACY_DEFAULT_OUTPUT_TYPE,
} from '../lib/utils/outputFormat.js';
import { sendRowsByOutputType } from '../lib/utils/routeHelpers.js';
import { getBasicLogger } from '../lib/utils/logger.js';

const router = express.Router();
const logger = getBasicLogger();

function getCdaRequestParams(req) {
  return req.method === 'GET' ? req.query ?? {} : req.body ?? {};
}

async function handleCdaDoQuery(req, res) {
  const requestParams = getCdaRequestParams(req);
  const { path, dataAccessId } = requestParams;

  if (!path || !dataAccessId) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const rawOutputType = requestParams.outputType || LEGACY_DEFAULT_OUTPUT_TYPE;

  if (!VALID_OUTPUT_TYPES.includes(rawOutputType)) {
    return res.status(400).json({
      error: 'BadRequest',
      description: `Invalid outputType '${rawOutputType}'. Allowed values: ${VALID_OUTPUT_TYPES.join(', ')}`,
    });
  }

  try {
    const result = await handleCdaQuery({
      body: requestParams,
      outputType: rawOutputType,
    });

    return sendRowsByOutputType(res, result, rawOutputType);
  } catch (err) {
    logger.error(
      {
        path,
        dataAccessId,
        err,
      },
      'Error executing query',
    );
    const status = err.status || 500;

    return res.status(status).json({
      error: err.type || 'InternalServerError',
      description: err.message || 'Unexpected error executing query',
    });
  }
}

/**
 * @swagger
 * /plugin/cda/api/doQuery:
 *   post:
 *     tags: [CDA Legacy]
 *     operationId: cdaDoQueryPost
 *     summary: Legacy Pentaho CDA query (POST)
 *     description: >
 *       Same behavior as the `GET` variant, with fields sent as an `application/x-www-form-urlencoded` body
 *       instead of query parameters.
 *     parameters:
 *       - $ref: '#/components/parameters/FiwareServiceHeaderOptional'
 *     requestBody:
 *       required: true
 *       content:
 *         application/x-www-form-urlencoded:
 *           schema:
 *             $ref: '#/components/schemas/CdaDoQueryFields'
 *           example:
 *             path: /public/trantor/verticals/sql/da1
 *             dataAccessId: da1
 *             pageSize: 10
 *     responses:
 *       '200':
 *         $ref: '#/components/responses/CdaQueryResult'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`FDANotFound` or `DaNotFound`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/plugin/cda/api/doQuery', handleCdaDoQuery);

/**
 * @swagger
 * /plugin/cda/api/doQuery:
 *   get:
 *     tags: [CDA Legacy]
 *     operationId: cdaDoQueryGet
 *     summary: Legacy Pentaho CDA query (GET)
 *     description: >
 *       Backward-compatibility endpoint for legacy Pentaho CDA clients. Wraps the internal FDA/DA query engine,
 *       adapting CDA-style requests and returning a CDA-compatible response shape. `visibility` and
 *       `servicePath` are resolved from `path` (`servicePath` is normalized as `/${visibility}`). The `Accept`
 *       header is ignored; response format is controlled solely by the `outputType` field. Query parameters
 *       prefixed with `param` (e.g. `parammunicipality=NA`) are forwarded as the target DA's query parameters.
 *     parameters:
 *       - $ref: '#/components/parameters/FiwareServiceHeaderOptional'
 *       - $ref: '#/components/parameters/CdaPathQuery'
 *       - $ref: '#/components/parameters/CdaDataAccessIdQuery'
 *       - $ref: '#/components/parameters/CdaOutputTypeQuery'
 *       - $ref: '#/components/parameters/CdaPageSizeQuery'
 *       - $ref: '#/components/parameters/CdaPageStartQuery'
 *     responses:
 *       '200':
 *         $ref: '#/components/responses/CdaQueryResult'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`FDANotFound` or `DaNotFound`.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.get('/plugin/cda/api/doQuery', handleCdaDoQuery);

export default router;
