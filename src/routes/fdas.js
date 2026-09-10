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
  getFDAs,
  fetchFDA,
  getFDA,
  updateFDA,
  deleteFDA,
  uploadFDA,
} from '../lib/fda.js';
import { config } from '../lib/fdaConfig.js';
import { uploadMiddleware } from '../lib/uploads.js';
import {
  validateAllowedFieldsBody,
  parseBooleanQueryParam,
  deleteTempFile,
} from '../lib/utils/utils.js';
import {
  parseValidationMode,
  validateFdaId,
} from '../lib/utils/routeHelpers.js';

const router = express.Router();

/**
 * @swagger
 * /{visibility}/fdas:
 *   get:
 *     tags: [FDAs]
 *     operationId: listFdas
 *     summary: List FDAs
 *     description: Returns all the FDAs for the given `service`, `servicePath` and `visibility`.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '200':
 *         description: >
 *           Array of FDAs. Each element includes `id` and excludes context/internal fields (`_id`, `fdaId`,
 *           `service`, `visibility`, `servicePath`) since those are already provided by the request scope.
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/FdaListItem'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 */
router.get('/:visibility/fdas', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility } = req.params;

  if (!service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fdas = await getFDAs(service, visibility, servicePath);
  return res.status(200).json(fdas);
});

/**
 * @swagger
 * /{visibility}/fdas:
 *   post:
 *     tags: [FDAs]
 *     operationId: createFda
 *     summary: Create FDA
 *     description: >
 *       Creates a new FDA. Processing is asynchronous: the response only confirms the request was accepted.
 *       `validationMode: "unchecked"` skips synchronous schema validation and automatic `defaultDataAccess`
 *       creation, useful for heavy queries.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *       - name: defaultDataAccess
 *         in: query
 *         required: false
 *         description: >
 *           Overrides the instance default (`FDA_CREATE_DEFAULT_DATA_ACCESS`, itself defaulting to `true`) and
 *           enables/disables automatic `defaultDataAccess` creation for this FDA. Ignored when the body's
 *           `validationMode` is `unchecked`.
 *         schema:
 *           type: boolean
 *         example: false
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/FdaCreateRequest'
 *           examples:
 *             interval:
 *               summary: Cached FDA with interval refresh
 *               value:
 *                 id: fda_alarms
 *                 datasourceId: default
 *                 query: SELECT * FROM public.alarms
 *                 description: FDA de alarmas del sistema
 *                 refreshPolicy:
 *                   type: interval
 *                   params:
 *                     refreshInterval: 1 hour
 *                 cached: true
 *             onlyFresh:
 *               summary: Only-fresh FDA (no parquet snapshot, no DAs)
 *               value:
 *                 id: fda_live_alarms
 *                 query: SELECT * FROM public.alarms
 *                 description: Only-fresh FDA
 *                 cached: false
 *             mongo:
 *               summary: Cached FDA over a MongoDB datasource
 *               value:
 *                 id: fda_mongo_events
 *                 datasourceId: mongo-default
 *                 query:
 *                   collection: events
 *                   filter:
 *                     site: lab
 *                   projection:
 *                     device: 1
 *                     status: 1
 *                     reading: 1
 *                 description: Mongo cached FDA
 *                 cached: true
 *     responses:
 *       '202':
 *         description: FDA creation accepted; processing continues asynchronously.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PendingStatus'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '404':
 *         description: '`DatasourceNotFound`'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '409':
 *         description: '`DuplicatedKey`: an FDA with this id already exists for the service.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/:visibility/fdas', async (req, res) => {
  const body = req.body ?? {};
  validateAllowedFieldsBody(body, [
    'id',
    'query',
    'description',
    'refreshPolicy',
    'timeColumn',
    'objStgConf',
    'cached',
    'datasourceId',
    'validationMode',
  ]);
  const {
    id,
    query,
    description,
    refreshPolicy,
    timeColumn,
    objStgConf,
    cached,
    datasourceId,
    validationMode,
  } = body;
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility } = req.params;
  const defaultDataAccessByConfig = config.defaultDataAccess?.enabled ?? true;
  const defaultDataAccessEnabled =
    req.query.defaultDataAccess === undefined
      ? defaultDataAccessByConfig
      : parseBooleanQueryParam(
          req.query.defaultDataAccess,
          'defaultDataAccess',
          true,
        );
  const cachedEnabled =
    cached === undefined
      ? true
      : parseBooleanQueryParam(cached, 'cached', true);
  const resolvedValidationMode = parseValidationMode(validationMode);

  if (!id || !query || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  validateFdaId(id);

  const finalRefreshPolicy = refreshPolicy ?? { type: 'none' };
  const finalObjStgConf = objStgConf ?? {};

  await fetchFDA(
    id,
    query,
    service,
    visibility,
    servicePath,
    description,
    finalRefreshPolicy,
    timeColumn,
    finalObjStgConf,
    defaultDataAccessEnabled,
    cachedEnabled,
    datasourceId,
    resolvedValidationMode,
  );

  return res.status(202).json({
    id,
    status: 'pending',
  });
});

function badRequest(res, req, description) {
  deleteTempFile(req.file);
  return res.status(400).json({
    error: 'BadRequest',
    description,
  });
}

function parseObjStgConf(objStgConf) {
  if (!objStgConf) {
    return {};
  }

  return typeof objStgConf === 'string' ? JSON.parse(objStgConf) : objStgConf;
}

function validateUploadRequest(req, res) {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');

  if (!service || !servicePath) {
    badRequest(
      res,
      req,
      'Missing Fiware-Service and Fiware-ServicePath headers',
    );
    return null;
  }

  const {
    id,
    description,
    timeColumn,
    objStgConf,
    defaultDataAccess,
    refreshPolicy,
  } = req.body;

  if (!id) {
    badRequest(res, req, 'Missing "id" field in the form');
    return null;
  }

  try {
    validateFdaId(id);
  } catch (error) {
    badRequest(res, req, error.message);
    return null;
  }

  if (!req.file) {
    badRequest(res, req, 'Missing file (form field "file")');
    return null;
  }

  const defaultDataAccessBool =
    defaultDataAccess === undefined
      ? config.defaultDataAccess?.enabled ?? true
      : parseBooleanQueryParam(defaultDataAccess, 'defaultDataAccess', true);

  let objStgConfParsed;
  try {
    objStgConfParsed = parseObjStgConf(objStgConf);
  } catch {
    badRequest(res, req, 'objStgConf must be a valid JSON object');
    return null;
  }

  if (refreshPolicy !== undefined) {
    let parsedRefreshPolicy;
    try {
      parsedRefreshPolicy =
        typeof refreshPolicy === 'string'
          ? JSON.parse(refreshPolicy)
          : refreshPolicy;
    } catch {
      badRequest(res, req, 'refreshPolicy must be a valid JSON object');
      return null;
    }

    if (parsedRefreshPolicy?.type !== 'none') {
      badRequest(
        res,
        req,
        'Upload FDAs cannot have a refreshPolicy; omit it or set {"type":"none"}',
      );
      return null;
    }
  }

  return {
    service,
    servicePath,
    id,
    description,
    timeColumn,
    defaultDataAccessBool,
    objStgConfParsed,
  };
}

/**
 * @swagger
 * /{visibility}/fdas/upload:
 *   post:
 *     tags: [FDAs]
 *     operationId: uploadFda
 *     summary: Create FDA from an uploaded tabular file
 *     description: >
 *       Creates a cached FDA from a tabular file (CSV, XLS or XLSX) using `multipart/form-data`. Always creates
 *       upload-based FDAs with `validationMode: "strict"`; `datasourceId` and `query` are stored as `null`.
 *       `refreshPolicy` is not allowed on uploads (omit it or set `{"type":"none"}`).
 *
 *
 *       Synchronous prevalidation returns `400` for invalid `objStgConf`/`refreshPolicy`, `415` for unsupported
 *       file types, and `413` for files above the configured size limit. If prevalidation passes, processing
 *       (parsing, Parquet conversion, optional default DA creation) continues asynchronously; semantic issues
 *       found while processing (e.g. a missing `timeColumn` in the uploaded headers) leave the FDA in
 *       `status: failed`.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/FdaUploadRequest'
 *           encoding:
 *             file:
 *               contentType: text/csv, application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
 *     responses:
 *       '202':
 *         description: Upload accepted; processing continues asynchronously.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PendingStatus'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '413':
 *         description: '`PayloadTooLarge`: the file exceeds `FDA_MAX_UPLOAD_SIZE` (50 MB by default).'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *       '415':
 *         description: '`UnsupportedMediaType`: the file is not CSV/XLS/XLSX.'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.post('/:visibility/fdas/upload', (req, res) => {
  uploadMiddleware(req, res, async (err) => {
    if (err) {
      deleteTempFile(req.file);

      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          error: 'PayloadTooLarge',
          description: 'The file exceeds the maximum allowed size.',
        });
      }

      return res.status(err.status || 400).json({
        error: err.type || 'BadRequest',
        description: err.message,
      });
    }

    const uploadData = validateUploadRequest(req, res);
    if (!uploadData) {
      return undefined;
    }

    const { visibility } = req.params;

    try {
      const result = await uploadFDA({
        fdaId: uploadData.id,
        tempFilePath: req.file.path,
        originalname: req.file.originalname,
        mimetype: req.file.mimetype,
        service: uploadData.service,
        visibility,
        servicePath: uploadData.servicePath,
        description: uploadData.description,
        timeColumn: uploadData.timeColumn,
        objStgConf: uploadData.objStgConfParsed,
        cached: true,
        defaultDataAccessEnabled: uploadData.defaultDataAccessBool,
      });

      return res.status(202).json({
        id: result.id,
        status: result.status,
      });
    } catch (error) {
      deleteTempFile(req.file);

      return res.status(error.status || 500).json({
        error: error.type || 'InternalServerError',
        description: error.message,
      });
    }
  });
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}:
 *   get:
 *     tags: [FDAs]
 *     operationId: getFda
 *     summary: Get FDA
 *     description: Returns the requested FDA.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '200':
 *         description: >
 *           FDA data, excluding redundant context/internal fields (`_id`, `fdaId`, `service`, `visibility`,
 *           `servicePath`, and here also `id`, which is already known from the request path).
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/FdaDetail'
 *       '400':
 *         $ref: '#/components/responses/BadRequest'
 *       '403':
 *         description: '`VisibilityMismatch`: the FDA exists but was created under a different `visibility`.'
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
 */
router.get('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!fdaId || !service || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  const fda = await getFDA(service, fdaId, visibility, servicePath);
  return res.status(200).json(fda);
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}:
 *   put:
 *     tags: [FDAs]
 *     operationId: regenerateFda
 *     summary: Regenerate FDA
 *     description: >
 *       Regenerates the FDA, fetching the source data again. Does not accept a request body. Returns `409` if
 *       the FDA is currently being processed, or if it is configured as only-fresh (`cached: false`, which does
 *       not support manual regeneration).
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '202':
 *         description: Regeneration accepted; processing continues asynchronously.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PendingStatus'
 *       '400':
 *         description: '`BadRequest`, including sending a non-empty request body (not accepted by this operation).'
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
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
 *       '409':
 *         description: FDA is currently processing, or is only-fresh and does not support regeneration.
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 */
router.put('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!service || !fdaId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  if (req.body && Object.keys(req.body).length > 0) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'PUT /fdas does not accept a request body',
    });
  }

  await updateFDA(service, fdaId, visibility, servicePath);

  return res.status(202).json({
    id: fdaId,
    status: 'pending',
  });
});

/**
 * @swagger
 * /{visibility}/fdas/{fdaId}:
 *   delete:
 *     tags: [FDAs]
 *     operationId: deleteFda
 *     summary: Delete FDA
 *     description: Deletes the FDA. Deleting an FDA cascades to delete all DAs belonging to it.
 *     parameters:
 *       - $ref: '#/components/parameters/VisibilityPath'
 *       - $ref: '#/components/parameters/FdaIdPath'
 *       - $ref: '#/components/parameters/FiwareServiceHeader'
 *       - $ref: '#/components/parameters/FiwareServicePathHeader'
 *     responses:
 *       '204':
 *         description: FDA (and its DAs) deleted.
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
 */
router.delete('/:visibility/fdas/:fdaId', async (req, res) => {
  const service = req.get('Fiware-Service');
  const servicePath = req.get('Fiware-ServicePath');
  const { visibility, fdaId } = req.params;

  if (!service || !fdaId || !servicePath || !visibility) {
    return res.status(400).json({
      error: 'BadRequest',
      description: 'Missing params in the request',
    });
  }

  await deleteFDA(service, fdaId, visibility, servicePath);
  return res.sendStatus(204);
});

export default router;
