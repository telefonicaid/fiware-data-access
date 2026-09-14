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

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import swaggerJsdoc from 'swagger-jsdoc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const packageInfo = JSON.parse(
  readFileSync(join(__dirname, '../../package.json'), 'utf8'),
);

export const openApiSpec = swaggerJsdoc({
  definition: {
    openapi: '3.0.3',
    info: {
      title: 'FIWARE Data Access API',
      version: packageInfo.version,
      description:
        'API exposed by the FIWARE Data Access (FDA) component to store and query analytical datasets ' +
        'backed by Parquet files in an object storage system (MinIO/S3), fetched from PostgreSQL or ' +
        'MongoDB datasources. See doc/03_api.md in the repository for the full narrative reference.',
      license: {
        name: 'AGPL-3.0-only',
        url: 'https://www.gnu.org/licenses/agpl-3.0.html',
      },
      contact: {
        name: 'FIWARE Data Access',
        url: 'https://github.com/telefonicaid/fiware-data-access',
      },
    },
    externalDocs: {
      description: 'Full Markdown API reference',
      url: 'https://github.com/telefonicaid/fiware-data-access/blob/main/doc/03_api.md',
    },
    servers: [
      { url: '/', description: 'Same host serving this documentation' },
    ],
    tags: [
      { name: 'Health', description: 'Service liveness/status.' },
      { name: 'Metrics', description: 'Prometheus/OpenMetrics telemetry.' },
      {
        name: 'Datasources',
        description:
          'Connection configuration (PostgreSQL/MongoDB) used to fetch FDAs, scoped by `Fiware-Service`.',
      },
      {
        name: 'FDAs',
        description:
          'Fetched Data Access resources (raw datasets materialized from a datasource).',
      },
      {
        name: 'DAs',
        description:
          'Data Access resources (stored parameterized queries over an FDA).',
      },
      {
        name: 'Data',
        description: 'Endpoints that execute queries and return result rows.',
      },
      {
        name: 'CDA Legacy',
        description:
          'Backward-compatible endpoint for legacy Pentaho CDA clients.',
      },
    ],
    // The API does not implement authentication/authorization; declared explicitly so tooling
    // doesn't flag it as an omission.
    security: [],
  },
  apis: [
    join(__dirname, 'openapiComponents.js'),
    join(__dirname, '../routes/health.js'),
    join(__dirname, '../routes/datasources.js'),
    join(__dirname, '../routes/fdas.js'),
    join(__dirname, '../routes/das.js'),
    join(__dirname, '../routes/data.js'),
    join(__dirname, '../routes/cdaLegacy.js'),
  ],
});
