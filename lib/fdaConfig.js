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

'use strict';

import Ajv from 'ajv';
import dotenv from 'dotenv';
import clone from 'lodash/clone.js';

dotenv.config();
const ajvSchema = new Ajv({ useDefaults: true });

const envVarsSchema = {
  type: 'object',
  properties: {
    NODE_ENV: {
      type: 'string',
      default: 'development',
      enum: ['development', 'production'],
    },
    FDA_POSTGRE_CLIENT_USER: {
      type: 'string',
      default: null,
    },
    FDA_POSTGRE_CLIENT_PASSWORD: {
      type: 'string',
      default: null,
    },
    FDA_POSTGRE_CLIENT_HOST: {
      type: 'string',
      default: null,
    },
    FDA_POSTGRE_CLIENT_PORT: {
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
    FDA_OBJSTG_ENDPOINT: {
      type: 'string',
      default: null,
    },
    FDA_MONGO_USER: {
      type: 'string',
      default: null,
    },
    FDA_MONGO_PASSWORD: {
      type: 'string',
      default: null,
    },
    FDA_MONGO_ENDPOINT: {
      type: 'string',
      default: null,
    },
  },
};

const envVars = clone(process.env);
const valid = ajvSchema
  .addSchema(envVarsSchema, 'envVarsSchema')
  .validate('envVarsSchema', envVars);

if (!valid) {
  console.log('Invalid envVars schema.');
}

export const config = {
  env: envVars.NODE_ENV,
  postgre_client: {
    usr: envVars.FDA_POSTGRE_CLIENT_USER,
    pass: envVars.FDA_POSTGRE_CLIENT_PASSWORD,
    host: envVars.FDA_POSTGRE_CLIENT_HOST,
    port: envVars.FDA_POSTGRE_CLIENT_PORT,
  },
  objstg: {
    usr: envVars.FDA_OBJSTG_USER,
    pass: envVars.FDA_OBJSTG_PASSWORD,
    endpoint: envVars.FDA_OBJSTG_ENDPOINT,
  },
  mongo: {
    usr: envVars.FDA_MONGO_USER,
    pass: envVars.FDA_MONGO_PASSWORD,
    endpoint: envVars.FDA_MONGO_ENDPOINT,
  },
};
