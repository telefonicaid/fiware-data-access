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

import logger from 'logops';
import { AsyncLocalStorage } from 'node:async_hooks';
import { v4 as uuidv4 } from 'uuid';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const packageInfo = JSON.parse(
  readFileSync(join(__dirname, '../../../package.json'), 'utf8'),
);
const requestLoggerStore = new AsyncLocalStorage();
let basicLoggerProxy = null;

function getCurrentLogger() {
  return requestLoggerStore.getStore() || logger;
}

export function initLogger(config) {
  logger.format = logger.formatters.pipe;
  logger.setLevel(config.logger.level);

  logger.getContext = () => ({
    ver: packageInfo.version,
    corr: 'n/a',
    trans: 'n/a',
    comp: config.logger.comp,
    op: 'n/a',
    srv: 'n/a',
    subsrv: 'n/a',
  });
}

export function getBasicLogger() {
  if (!basicLoggerProxy) {
    basicLoggerProxy = new Proxy(logger, {
      get(target, property) {
        const currentLogger = getCurrentLogger();
        const value = currentLogger[property] ?? target[property];
        return typeof value === 'function' ? value.bind(currentLogger) : value;
      },
    });
  }

  return basicLoggerProxy;
}

export function runWithLogger(requestLogger, callback) {
  return requestLoggerStore.run(requestLogger, callback);
}

export function createChildLogger(config) {
  const loggerCtx = logger.getContext();
  return logger.child({
    op: config?.op || loggerCtx.op,
    corr: config?.corr || uuidv4(),
    trans: config?.trans || uuidv4(),
    srv: config?.service || 'n/a',
    subsrv: config?.subservice || 'n/a',
  });
}

export function getInitialLogger(config) {
  return logger.child({
    envVars: `[objStgHost=${
      config.objstg.protocol + '://' + config.objstg.endpoint
    } objStgUsr=${config.objstg.usr}]`,
    dependencies: `@aws-sdk/lib-storage:${packageInfo.dependencies['@aws-sdk/lib-storage']} @duckdb/node-api:${packageInfo.dependencies['@duckdb/node-api']} express:${packageInfo.dependencies.express} mongodb:${packageInfo.dependencies.mongodb} pg:${packageInfo.dependencies.pg}`,
  });
}
