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

// Single source of truth for enum-like constants shared across src/lib and src/routes.

// Datasources
export const DEFAULT_DATASOURCE_ID = 'default';
export const SUPPORTED_DATASOURCE_TYPES = new Set(['postgres', 'mongodb']);
export const DISALLOWED_MONGO_AGGREGATION_STAGES = new Set(['$out', '$merge']);

// FDA validation modes
export const FDA_VALIDATION_MODE_STRICT = 'strict';
export const FDA_VALIDATION_MODE_UNCHECKED = 'unchecked';
export const VALIDATION_MODES = [
  FDA_VALIDATION_MODE_STRICT,
  FDA_VALIDATION_MODE_UNCHECKED,
];

// Visibility
export const VALID_VISIBILITIES = ['public', 'private'];
export const VALID_VISIBILITIES_SET = new Set(VALID_VISIBILITIES);

// Refresh policy / fresh window fetch sizes
export const VALID_REFRESH_POLICY_TYPES = ['none', 'interval', 'window'];
export const VALID_WINDOW_FETCH_SIZES = [
  'hour',
  'day',
  'week',
  'month',
  'year',
];

// Storage partitions
export const PARTITION_TYPES = ['day', 'week', 'month', 'year', 'none'];

// Output formats
export const VALID_OUTPUT_TYPES = ['ndjson', 'json', 'csv', 'xls'];
export const DEFAULT_OUTPUT_TYPE = 'ndjson';
export const LEGACY_DEFAULT_OUTPUT_TYPE = 'json';
export const QUERY_STYLE_OUTPUT_TYPES = ['json', 'ndjson', 'csv', 'xls', 'cda'];

// Supported MIME types for /data, listed in server-default preference order.
export const DATA_CONTENT_TYPES = [
  'application/json',
  'application/x-ndjson',
  'text/csv',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.ms-excel',
  'application/vnd.fiware.cda+json',
];
