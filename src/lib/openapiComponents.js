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

/**
 * @swagger
 * components:
 *   parameters:
 *     VisibilityPath:
 *       name: visibility
 *       in: path
 *       required: true
 *       description: FDA access visibility.
 *       schema:
 *         type: string
 *         enum: [public, private]
 *       example: public
 *     FdaIdPath:
 *       name: fdaId
 *       in: path
 *       required: true
 *       description: Id of the FDA.
 *       schema:
 *         type: string
 *       example: fda_alarms
 *     DaIdPath:
 *       name: daId
 *       in: path
 *       required: true
 *       description: Id of the DA.
 *       schema:
 *         type: string
 *       example: da_all_alarms
 *     DatasourceIdPath:
 *       name: datasourceId
 *       in: path
 *       required: true
 *       description: Datasource identifier within the service.
 *       schema:
 *         type: string
 *       example: default
 *     FiwareServiceHeader:
 *       name: Fiware-Service
 *       in: header
 *       required: true
 *       description: Tenant or service, using the common mechanism of the FIWARE platform.
 *       schema:
 *         type: string
 *       example: trantor
 *     FiwareServicePathHeader:
 *       name: Fiware-ServicePath
 *       in: header
 *       required: true
 *       description: NGSI hierarchical service path. Must be a non-root absolute path (e.g. `/servicePath`).
 *       schema:
 *         type: string
 *       example: /servicePath
 *     FiwareServiceHeaderOptional:
 *       name: Fiware-Service
 *       in: header
 *       required: false
 *       description: >
 *         Tenant/service for **header-style** context. Required unless the equivalent `service` query parameter
 *         is used instead; mixing both styles returns `409 RequestStyleConflict`.
 *       schema:
 *         type: string
 *       example: trantor
 *     FiwareServicePathHeaderOptional:
 *       name: Fiware-ServicePath
 *       in: header
 *       required: false
 *       description: >
 *         Service path for **header-style** context. Required unless the equivalent `servicePath` query parameter
 *         is used instead; mixing both styles returns `409 RequestStyleConflict`.
 *       schema:
 *         type: string
 *       example: /servicePath
 *     ServiceQueryOptional:
 *       name: service
 *       in: query
 *       required: false
 *       description: >
 *         Tenant/service for **query-style** context. Required (together with `servicePath`) when not using the
 *         `Fiware-Service`/`Fiware-ServicePath` headers instead.
 *       schema:
 *         type: string
 *       example: trantor
 *     ServicePathQueryOptional:
 *       name: servicePath
 *       in: query
 *       required: false
 *       description: Service path for **query-style** context. Required together with `service`.
 *       schema:
 *         type: string
 *       example: /servicePath
 *     OutputTypeQueryOptional:
 *       name: outputType
 *       in: query
 *       required: false
 *       description: Output format used in **query-style** context. Ignored (and rejected) in header-style context.
 *       schema:
 *         type: string
 *         enum: [json, ndjson, csv, xls, cda]
 *         default: ndjson
 *       example: csv
 *     AcceptDataHeader:
 *       name: Accept
 *       in: header
 *       required: false
 *       description: >
 *         Response format for **header-style** context, negotiated via standard HTTP content negotiation.
 *         Missing or a wildcard value resolves to `application/json`.
 *       schema:
 *         type: string
 *         enum:
 *           - application/json
 *           - application/x-ndjson
 *           - text/csv
 *           - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
 *           - application/vnd.ms-excel
 *           - application/vnd.fiware.cda+json
 *       example: application/json
 *     CdaPathQuery:
 *       name: path
 *       in: query
 *       required: true
 *       description: >
 *         Path used to resolve context (`visibility`, `service`, and FDA id). Supported formats include
 *         `/public/<service>/...` and `home/<service>/verticals/public/<fda>.cda`. If no explicit FDA id is
 *         present, it defaults to `dataAccessId`.
 *       schema:
 *         type: string
 *       example: /public/trantor/verticals/sql/fda1
 *     CdaDataAccessIdQuery:
 *       name: dataAccessId
 *       in: query
 *       required: true
 *       description: Identifier of the Data Access (DA) inside the FDA.
 *       schema:
 *         type: string
 *       example: da1
 *     CdaOutputTypeQuery:
 *       name: outputType
 *       in: query
 *       required: false
 *       description: >
 *         Format of the returned results. `ndjson` is accepted but behaves the same as `json` on this endpoint
 *         (both return the CDA-compatible structure, not line-delimited output).
 *       schema:
 *         type: string
 *         enum: [json, ndjson, csv, xls]
 *         default: json
 *       example: json
 *     CdaPageSizeQuery:
 *       name: pageSize
 *       in: query
 *       required: false
 *       description: Pagination size. Must be handled explicitly by the DA query; if omitted, DA defaults apply.
 *       schema:
 *         type: integer
 *       example: 10
 *     CdaPageStartQuery:
 *       name: pageStart
 *       in: query
 *       required: false
 *       description: Pagination offset. Must be handled explicitly by the DA query; if omitted, DA defaults apply.
 *       schema:
 *         type: integer
 *       example: 0
 *
 *   responses:
 *     BadRequest:
 *       description: >
 *         `BadRequest`: missing/invalid values in body, headers or query parameters (`Fiware-Service`,
 *         `Fiware-ServicePath` and `visibility` are required for all tenant-scoped operations).
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/Error'
 *     NotAcceptable:
 *       description: '`NotAcceptable`: the `Accept` header does not allow any supported response format.'
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/Error'
 *     DataQueryResult:
 *       description: >
 *         Query results. In header-style context the format is chosen via content negotiation (`Accept`); in
 *         query-style context it is chosen via `outputType` (default `ndjson`). `ndjson` and `csv` responses are
 *         streamed incrementally.
 *       content:
 *         application/json:
 *           schema:
 *             type: array
 *             items:
 *               type: object
 *             description: One JSON object per result record.
 *         application/x-ndjson:
 *           schema:
 *             type: string
 *             description: One JSON object per line (streamed).
 *         text/csv:
 *           schema:
 *             type: string
 *             description: Comma-separated values, quoted where needed. First row contains column names.
 *         application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
 *           schema:
 *             type: string
 *             format: binary
 *             description: Excel workbook (.xlsx, Office Open XML). First row contains column names.
 *         application/vnd.fiware.cda+json:
 *           schema:
 *             allOf:
 *               - $ref: '#/components/schemas/CdaResult'
 *             description: >
 *               CDA-compatible JSON structure. Note the actual `Content-Type` response header is `application/json`,
 *               not `application/vnd.fiware.cda+json`.
 *     CdaQueryResult:
 *       description: Query results in the format selected by `outputType`.
 *       content:
 *         application/json:
 *           schema:
 *             allOf:
 *               - $ref: '#/components/schemas/CdaResult'
 *             description: >
 *               Used for both `outputType=json` and `outputType=ndjson` (identical on this legacy endpoint; always
 *               the CDA-compatible `{ metadata, resultset, queryInfo }` structure, never a plain array).
 *         text/csv:
 *           schema:
 *             type: string
 *             description: Comma-separated values with column names in the first row.
 *         application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
 *           schema:
 *             type: string
 *             format: binary
 *             description: Excel workbook (.xlsx) with column names in the first row.
 *
 *   schemas:
 *     Error:
 *       type: object
 *       required: [error]
 *       properties:
 *         error:
 *           type: string
 *           description: Machine-readable error code.
 *           example: BadRequest
 *         description:
 *           type: string
 *           description: >
 *             Additional human-readable information. Exact wording may vary between FDA versions; do not rely on
 *             it for programmatic logic.
 *           example: Missing params in the request
 *
 *     PendingStatus:
 *       type: object
 *       properties:
 *         id:
 *           type: string
 *           example: fda_alarms
 *         status:
 *           type: string
 *           example: pending
 *
 *     RefreshPolicyParams:
 *       type: object
 *       description: Parameters for the refresh policy. Not used when `type` is `none`, mandatory otherwise.
 *       properties:
 *         refreshInterval:
 *           type: string
 *           description: >
 *             Human interval (e.g. "1 hour") or cron expression. Frequency for the scheduled refresh/clean jobs.
 *             Must be less than or equal to the partition size, if configured.
 *           example: 1 hour
 *         consistencyRefreshInterval:
 *           type: string
 *           description: >
 *             Only for type `window`. Human interval or cron expression used to schedule a periodic full rebuild
 *             of the FDA, to recover delayed historical data while keeping incremental refresh active. Must be
 *             greater than `refreshInterval`.
 *           example: 1 day
 *         fetchSize:
 *           type: string
 *           enum: [hour, day, week, month, year]
 *           description: >
 *             Only for type `window`. Time range fetched on each scheduled run. Must equal the partition size when
 *             `objStgConf.partition` is set; `hour` is only valid without partitioning.
 *         windowSize:
 *           type: string
 *           description: >
 *             Temporal interval of data kept in storage (e.g. "3 days", "6 weeks", "1 month", "2 years"; a unit
 *             without a number counts as amount 1). Also affects the first fetch and manual regeneration. If
 *             omitted, all data is kept forever (no clean partition is done).
 *           example: 1 month
 *       example:
 *         refreshInterval: 1 hour
 *
 *     RefreshPolicy:
 *       type: object
 *       description: 'Policy for automatic FDA refresh. Defaults to `{ "type": "none" }` when omitted.'
 *       required: [type]
 *       properties:
 *         type:
 *           type: string
 *           enum: [none, interval, window]
 *         params:
 *           $ref: '#/components/schemas/RefreshPolicyParams'
 *
 *     ObjStgConf:
 *       type: object
 *       description: Options to configure the FDA uploaded to the object storage system.
 *       properties:
 *         partition:
 *           type: string
 *           enum: [day, week, month, year]
 *           description: How FDA data is partitioned in object storage. Default is no partitioning.
 *         compression:
 *           type: boolean
 *           default: false
 *           description: Whether the FDA Parquet file is compressed using ZSTD compression.
 *
 *     Param:
 *       type: object
 *       description: Controls a single query parameter accepted by a DA.
 *       required: [name, type]
 *       properties:
 *         name:
 *           type: string
 *           description: Name of the param to control.
 *         type:
 *           type: string
 *           enum: [Number, Boolean, Text, DateTime]
 *         required:
 *           type: boolean
 *           default: false
 *           description: >
 *             Whether the param must be provided by the client. If `true` and the value is missing, the request
 *             fails even when `default` is defined.
 *         default:
 *           description: >
 *             Default value applied only when the value is missing and `required` is `false`. Can be `null` for
 *             any `type`, in which case `NULL` is bound to the query, useful for optional filters such as
 *             `WHERE ($param IS NULL OR field = $param)`.
 *         range:
 *           type: array
 *           description: '`[min, max]` for `Number` params; the first value must be lower than the second.'
 *           items:
 *             type: number
 *           minItems: 2
 *           maxItems: 2
 *         enum:
 *           type: array
 *           description: All possible values (`Number` or `Text`) the param can take.
 *           items: {}
 *       example:
 *         name: activity
 *         type: Number
 *         required: true
 *         range: [10, 14]
 *
 *     MongoQuery:
 *       type: object
 *       description: >
 *         Mongo query definition used when the FDA's datasource type is `mongodb`. `filter` and `aggregation` are
 *         mutually exclusive; exactly one must be provided. Aggregation pipelines are read-only (`$out`/`$merge`
 *         stages are not allowed).
 *       required: [collection]
 *       properties:
 *         collection:
 *           type: string
 *           description: MongoDB collection name.
 *         filter:
 *           type: object
 *           description: MongoDB filter document. Mutually exclusive with `aggregation`.
 *         projection:
 *           type: object
 *           description: >
 *             MongoDB projection document defining the fields materialized into the FDA (used with `filter`).
 *             Nested fields can be projected using dot notation (e.g. `device.name`) and are materialized
 *             preserving that dot notation; in generated `defaultDataAccess` parameters, dots are replaced by
 *             underscores (e.g. `device_name`).
 *         aggregation:
 *           type: array
 *           description: MongoDB aggregation pipeline. Mutually exclusive with `filter`. Each stage is a single-key object.
 *           items:
 *             type: object
 *       example:
 *         collection: sensors
 *         aggregation:
 *           - $match: { site: lab }
 *           - $group: { _id: $category, n: { $sum: 1 } }
 *
 *     FdaCreateRequest:
 *       type: object
 *       description: >
 *         FDA payload accepted by `POST /{visibility}/fdas`. Read-only/operational fields (`status`, `progress`,
 *         `initFetch`, `lastFetch`) must not be included and are rejected with `400 BadRequest`.
 *       additionalProperties: false
 *       required: [id, query]
 *       properties:
 *         id:
 *           type: string
 *           pattern: '^[a-zA-Z0-9_-]+$'
 *           description: FDA unique identifier. Only alphanumeric characters, hyphens and underscores.
 *           example: fda_alarms
 *         query:
 *           description: SQL query string for `postgres` datasources, or a Mongo query definition for `mongodb` datasources.
 *           oneOf:
 *             - type: string
 *             - $ref: '#/components/schemas/MongoQuery'
 *           example: SELECT * FROM public.alarms
 *         description:
 *           type: string
 *           description: Free text describing the FDA. If omitted, no description is stored.
 *         refreshPolicy:
 *           $ref: '#/components/schemas/RefreshPolicy'
 *         timeColumn:
 *           type: string
 *           description: >
 *             Required with `refreshPolicy` of type `window` or `partition`. Column indicating when the data was
 *             received. In `strict` validation mode it must be explicitly projected in the query's SELECT clause.
 *         objStgConf:
 *           $ref: '#/components/schemas/ObjStgConf'
 *         cached:
 *           type: boolean
 *           default: true
 *           description: >
 *             If `false`, the FDA is created as only-fresh: no Parquet snapshot is maintained, no DAs are allowed,
 *             and the FDA is queried through `GET /{visibility}/fdas/{fdaId}/data`.
 *         datasourceId:
 *           type: string
 *           default: default
 *           description: Datasource id used to resolve DB credentials for this FDA.
 *         validationMode:
 *           type: string
 *           enum: [strict, unchecked]
 *           default: strict
 *           description: >
 *             Controls synchronous validation during creation. `unchecked` skips validation and initial
 *             `defaultDataAccess` generation, useful for heavy queries.
 *
 *     FdaUploadRequest:
 *       type: object
 *       description: Multipart form fields accepted by `POST /{visibility}/fdas/upload`.
 *       required: [id, file]
 *       properties:
 *         id:
 *           type: string
 *           description: >
 *             FDA identifier. Allowed-character validation happens during asynchronous processing; the API
 *             accepts the value and returns `202 Accepted` even for an ultimately invalid id.
 *           example: upload_weather
 *         file:
 *           type: string
 *           format: binary
 *           description: CSV/XLS/XLSX file content (form field must be named `file`).
 *         description:
 *           type: string
 *           description: Optional FDA description.
 *         timeColumn:
 *           type: string
 *           description: >
 *             Time column name. Presence and type are validated asynchronously; the API accepts the value on
 *             upload.
 *         objStgConf:
 *           description: JSON-encoded `ObjStgConf` object, or the object itself.
 *           oneOf:
 *             - type: string
 *             - $ref: '#/components/schemas/ObjStgConf'
 *         defaultDataAccess:
 *           type: boolean
 *           description: Overrides default DA auto-creation for this upload.
 *         refreshPolicy:
 *           description: >
 *             Not allowed on uploads besides the no-op default; omit it or send `{"type":"none"}`, otherwise the
 *             request is rejected with `400 BadRequest`.
 *           oneOf:
 *             - type: string
 *             - $ref: '#/components/schemas/RefreshPolicy'
 *
 *     FdaFields:
 *       type: object
 *       description: Fields common to both FDA response representations (list item and single-FDA detail).
 *       properties:
 *         datasourceId:
 *           type: string
 *         validationMode:
 *           type: string
 *           enum: [strict, unchecked]
 *         query:
 *           description: >
 *             SQL query string for `postgres` datasources, a Mongo query definition (see `MongoQuery`) for
 *             `mongodb` datasources, or `null` for datasource-less FDAs created from uploaded tabular files.
 *         description:
 *           type: string
 *         das:
 *           type: object
 *           description: Map of DA ids belonging to this FDA.
 *           additionalProperties: true
 *         status:
 *           type: string
 *           enum: [fetching, transforming, uploading, completed, failed]
 *         progress:
 *           type: number
 *           description: Execution progress percentage (0-100).
 *         initFetch:
 *           type: string
 *           format: date-time
 *           description: Timestamp of the current/last fetch start.
 *         lastFetch:
 *           type: string
 *           format: date-time
 *           description: Timestamp of the last completed fetch.
 *         refreshPolicy:
 *           $ref: '#/components/schemas/RefreshPolicy'
 *         schema:
 *           type: array
 *           description: Column schema inferred/validated for the FDA.
 *           items:
 *             type: object
 *             properties:
 *               name:
 *                 type: string
 *               type:
 *                 type: string
 *
 *     FdaListItem:
 *       description: FDA representation returned by `GET /{visibility}/fdas` (includes `id`).
 *       allOf:
 *         - type: object
 *           properties:
 *             id:
 *               type: string
 *               example: fda_alarms
 *         - $ref: '#/components/schemas/FdaFields'
 *
 *     FdaDetail:
 *       description: >
 *         FDA representation returned by `GET /{visibility}/fdas/{fdaId}` (excludes `id`, already known from the
 *         request path).
 *       allOf:
 *         - $ref: '#/components/schemas/FdaFields'
 *
 *     Da:
 *       type: object
 *       properties:
 *         id:
 *           type: string
 *           example: da_all_alarms
 *         description:
 *           type: string
 *           example: Todas las alarmas
 *         query:
 *           type: string
 *           description: >
 *             Query string, without a `FROM` clause, run over the FDA when invoking the DA. Plain SQL: any clause
 *             supported by the underlying engine can be used, including `LIKE` with wildcards.
 *           example: SELECT * LIMIT 10
 *         params:
 *           type: array
 *           description: Controls param values. If omitted, the DA has no query parameters.
 *           items:
 *             $ref: '#/components/schemas/Param'
 *
 *     DaCreateRequest:
 *       type: object
 *       additionalProperties: false
 *       required: [id, query]
 *       properties:
 *         id:
 *           type: string
 *           pattern: '^[a-zA-Z0-9_-]+$'
 *           description: DA identifier, unique within the associated FDA.
 *         description:
 *           type: string
 *         query:
 *           type: string
 *         params:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/Param'
 *
 *     DaUpdateRequest:
 *       type: object
 *       description: '`id` must not be included when updating a DA.'
 *       additionalProperties: false
 *       required: [query]
 *       properties:
 *         description:
 *           type: string
 *         query:
 *           type: string
 *         params:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/Param'
 *
 *     PostgresDatasourceConfig:
 *       type: object
 *       description: Connection configuration for a `postgres` datasource.
 *       additionalProperties: false
 *       required: [username, password, host, port, database]
 *       properties:
 *         username:
 *           type: string
 *         password:
 *           type: string
 *         host:
 *           type: string
 *         port:
 *           type: integer
 *         database:
 *           type: string
 *       example:
 *         username: postgres
 *         password: postgres
 *         host: localhost
 *         port: 5432
 *         database: trantor
 *
 *     MongoDatasourceConfig:
 *       type: object
 *       description: Connection configuration for a `mongodb` datasource.
 *       additionalProperties: false
 *       required: [uri, database]
 *       properties:
 *         uri:
 *           type: string
 *         database:
 *           type: string
 *       example:
 *         uri: 'mongodb://localhost:27017'
 *         database: trantor
 *
 *     Datasource:
 *       type: object
 *       properties:
 *         datasourceId:
 *           type: string
 *           nullable: true
 *           description: >
 *             Datasource identifier, unique within a given `Fiware-Service`. `null` means the FDA does not use a
 *             datasource (as for FDAs created from uploaded tabular files); not applicable to datasource resources
 *             themselves, which always carry an id.
 *           example: default
 *         type:
 *           type: string
 *           enum: [postgres, mongodb]
 *         config:
 *           oneOf:
 *             - $ref: '#/components/schemas/PostgresDatasourceConfig'
 *             - $ref: '#/components/schemas/MongoDatasourceConfig'
 *
 *     DatasourceCreateRequest:
 *       type: object
 *       additionalProperties: false
 *       required: [type, config]
 *       properties:
 *         datasourceId:
 *           type: string
 *           pattern: '^[a-zA-Z0-9_-]+$'
 *           default: default
 *           description: Defaults to `"default"` when not provided.
 *         type:
 *           type: string
 *           enum: [postgres, mongodb]
 *         config:
 *           oneOf:
 *             - $ref: '#/components/schemas/PostgresDatasourceConfig'
 *             - $ref: '#/components/schemas/MongoDatasourceConfig'
 *
 *     DatasourceUpdateRequest:
 *       type: object
 *       description: Fields present are validated (by opening a connection) and stored; omitted fields are left unchanged.
 *       additionalProperties: false
 *       properties:
 *         type:
 *           type: string
 *           enum: [postgres, mongodb]
 *         config:
 *           oneOf:
 *             - $ref: '#/components/schemas/PostgresDatasourceConfig'
 *             - $ref: '#/components/schemas/MongoDatasourceConfig'
 *
 *     CdaDoQueryFields:
 *       type: object
 *       required: [path, dataAccessId]
 *       properties:
 *         path:
 *           type: string
 *           example: /public/trantor/verticals/sql/fda1
 *         dataAccessId:
 *           type: string
 *           example: da1
 *         outputType:
 *           type: string
 *           enum: [json, ndjson, csv, xls]
 *           default: json
 *         pageSize:
 *           type: integer
 *         pageStart:
 *           type: integer
 *       description: >
 *         Additional `param<name>=value` fields (e.g. `parammunicipality=NA`) are forwarded as the target DA's
 *         query parameters; they are not enumerable here since their names depend on the DA being invoked.
 *
 *     CdaResult:
 *       type: object
 *       description: CDA-compatible tabular JSON structure.
 *       properties:
 *         metadata:
 *           type: array
 *           items:
 *             type: object
 *             properties:
 *               colIndex:
 *                 type: integer
 *               colName:
 *                 type: string
 *         resultset:
 *           type: array
 *           description: One array of stringified values per result row, ordered as in `metadata`.
 *           items:
 *             type: array
 *             items: {}
 *         queryInfo:
 *           type: object
 *           properties:
 *             pageStart:
 *               type: integer
 *             pageSize:
 *               type: integer
 *             totalRows:
 *               type: integer
 *       example:
 *         metadata:
 *           - colIndex: 0
 *             colName: column1
 *         resultset:
 *           - [value1, value2]
 *         queryInfo:
 *           pageStart: 0
 *           pageSize: 10
 *           totalRows: 2
 *
 *     HealthPayload:
 *       type: object
 *       properties:
 *         status:
 *           type: string
 *           example: UP
 *         timestamp:
 *           type: string
 *           format: date-time
 *         uptimeSeconds:
 *           type: number
 *         process:
 *           type: object
 *           properties:
 *             pid:
 *               type: integer
 *             nodeVersion:
 *               type: string
 *             memory:
 *               type: object
 *               properties:
 *                 rssBytes:
 *                   type: integer
 *                 heapTotalBytes:
 *                   type: integer
 *                 heapUsedBytes:
 *                   type: integer
 *         roles:
 *           type: object
 *           properties:
 *             apiServer:
 *               type: boolean
 *             fetcher:
 *               type: boolean
 *             syncQueries:
 *               type: boolean
 *         traffic:
 *           type: object
 *           properties:
 *             totalRequests:
 *               type: integer
 *             errorRequests:
 *               type: integer
 *             inFlightRequests:
 *               type: integer
 *             routesObserved:
 *               type: integer
 *         fiware:
 *           type: object
 *           properties:
 *             requestsWithHeaders:
 *               type: integer
 *             servicesObserved:
 *               type: integer
 *             servicePathsObserved:
 *               type: integer
 *         mongo:
 *           type: object
 *           properties:
 *             scrapeOk:
 *               type: boolean
 *             source:
 *               type: string
 *               example: live
 *             lastSuccessTimestamp:
 *               type: string
 *               format: date-time
 *             lastError:
 *               type: string
 *             fdasTotal:
 *               type: integer
 *             dasTotal:
 *               type: integer
 *             agendaJobsTotal:
 *               type: integer
 *             agendaJobsFailed:
 *               type: integer
 *             agendaJobsLocked:
 *               type: integer
 *       example:
 *         status: UP
 *         timestamp: '2026-02-16T10:15:30.123Z'
 *         uptimeSeconds: 154
 *         process:
 *           pid: 3210
 *           nodeVersion: v24.0.0
 *           memory:
 *             rssBytes: 85422080
 *             heapTotalBytes: 33230848
 *             heapUsedBytes: 19459968
 *         roles:
 *           apiServer: true
 *           fetcher: true
 *           syncQueries: false
 *         traffic:
 *           totalRequests: 105
 *           errorRequests: 3
 *           inFlightRequests: 0
 *           routesObserved: 8
 *         fiware:
 *           requestsWithHeaders: 98
 *           servicesObserved: 3
 *           servicePathsObserved: 4
 *         mongo:
 *           scrapeOk: true
 *           source: live
 *           lastSuccessTimestamp: '2026-04-06T08:52:58.595Z'
 *           fdasTotal: 2
 *           dasTotal: 1
 *           agendaJobsTotal: 0
 *           agendaJobsFailed: 0
 *           agendaJobsLocked: 0
 */
export {};
