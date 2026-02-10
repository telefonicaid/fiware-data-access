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

import Ajv from 'ajv';
import dotenv from 'dotenv';
import clone from 'lodash/clone.js';
import { getBasicLogger } from './utils/logger.js';

dotenv.config();
const ajvSchema = new Ajv({ useDefaults: true, coerceTypes: true });

const envVarsSchema = {
  type: 'object',
  properties: {
    FDA_NODE_ENV: {
      type: 'string',
      default: 'development',
      enum: ['development', 'production'],
    },
    FDA_SERVER_PORT: {
      type: 'number',
      default: 8080,
    },
    FDA_PG_USER: {
      type: 'string',
      default: null,
    },
    FDA_PG_PASSWORD: {
      type: 'string',
      default: null,
    },
    FDA_PG_HOST: {
      type: 'string',
      default: null,
    },
    FDA_PG_PORT: {
      type: 'number',
      default: 5432,
    },
    FDA_OBJSTG_USER: {
      type: 'string',
      default: null,
    },
    FDA_OBJSTG_PASSWORD: {
      type: 'string',
      default: null,
    },
    FDA_OBJSTG_PROTOCOL: {
      type: 'string',
      default: 'https',
    },
    FDA_OBJSTG_ENDPOINT: {
      type: 'string',
      default: null,
    },
    FDA_MONGO_URI: {
      type: 'string',
      default: null,
    },
    FDA_LOG_LEVEL: {
      type: 'string',
      default: 'INFO',
      enum: ['INFO', 'WARN', 'ERROR', 'DEBUG'],
    },
    FDA_LOG_COMP: {
      type: 'string',
      default: 'FDA',
    },
    FDA_LOG_RES_SIZE: {
      type: 'number',
      default: 100,
    },
  },
};

const envVars = clone(process.env);
const valid = ajvSchema
  .addSchema(envVarsSchema, 'envVarsSchema')
  .validate('envVarsSchema', envVars);

if (!valid) {
  getBasicLogger().error(new Error(ajvSchema.errorsText()));
}

export const config = {
  env: envVars.FDA_NODE_ENV,
  port: envVars.FDA_SERVER_PORT,
  pg: {
    usr: envVars.FDA_PG_USER,
    pass: envVars.FDA_PG_PASSWORD,
    host: envVars.FDA_PG_HOST,
    port: envVars.FDA_PG_PORT,
  },
  objstg: {
    usr: envVars.FDA_OBJSTG_USER,
    pass: envVars.FDA_OBJSTG_PASSWORD,
    protocol: envVars.FDA_OBJSTG_PROTOCOL,
    endpoint: envVars.FDA_OBJSTG_ENDPOINT,
  },
  mongo: {
    uri: envVars.FDA_MONGO_URI,
  },
  logger: {
    level: envVars.FDA_LOG_LEVEL,
    comp: envVars.FDA_LOG_COMP,
    resSize: envVars.FDA_LOG_RES_SIZE,
  },
};
