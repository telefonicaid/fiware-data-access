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
import fs from 'node:fs';
import path from 'node:path';

import { startFetcher } from './fetcher.js';
import { shutdownAgenda, initAgenda } from './lib/jobs.js';
import { createIndex, disconnectClient } from './lib/utils/mongo.js';
import { destroyS3Client } from './lib/utils/aws.js';
import { closePgPools } from './lib/utils/pg.js';
import { config } from './lib/fdaConfig.js';
import { UPLOAD_TMP_DIR, ensureUploadTmpDir } from './lib/uploads.js';
import {
  initLogger,
  getBasicLogger,
  createChildLogger,
  runWithLogger,
  getInitialLogger,
} from './lib/utils/logger.js';
import { onRequestStart, onRequestFinish } from './lib/metrics.js';
import apiDocsRouter from './routes/apiDocs.js';
import healthRouter from './routes/health.js';
import datasourcesRouter from './routes/datasources.js';
import fdasRouter from './routes/fdas.js';
import dasRouter from './routes/das.js';
import dataRouter from './routes/data.js';
import cdaLegacyRouter from './routes/cdaLegacy.js';

export const app = express();
const PORT = config.port;
const logger = getBasicLogger();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  const reqStartMs = onRequestStart();
  req.log = createChildLogger({
    corr: req.get('Fiware-Correlator'),
    service: req.get('Fiware-Service') || 'n/a',
    subservice: req.get('Fiware-ServicePath') || 'n/a',
  });
  const oldSend = res.send;
  const oldJson = res.json;

  function capture(body) {
    try {
      let str = typeof body === 'string' ? body : JSON.stringify(body);

      if (str.length > config.logger.resSize) {
        str = str.slice(0, config.logger.resSize) + '…[truncated]';
      }

      res.locals.responseBody = str;
    } catch {
      res.locals.responseBody = '[unserializable body]';
    }
  }

  res.send = function (body) {
    capture(body);
    return oldSend.call(this, body);
  };

  res.json = function (body) {
    capture(body);
    return oldJson.call(this, body);
  };

  const start = Date.now();

  res.on('finish', () => {
    onRequestFinish(req, res, reqStartMs);

    req.log.info(
      {
        method: req.method,
        path: req.originalUrl,
        reqParams: `${JSON.stringify(req.params)}`,
        reqQuery: `${JSON.stringify(req.query)}`,
        reqBody: `${JSON.stringify(req.body)}`,
        resCode: res.statusCode,
        resMsg: res.statusMessage,
        durationMs: Date.now() - start,
        ip: req.ip,
        userAgent: req.headers['user-agent'],
        resSize: res.getHeader('Content-Length'),
        resBody: res.locals.responseBody,
      },
      'API request completed',
    );
  });

  return runWithLogger(req.log, () => next());
});

app.use(healthRouter);
app.use(apiDocsRouter);
app.use(fdasRouter);
app.use(dasRouter);
app.use(dataRouter);
app.use(datasourcesRouter);
app.use(cdaLegacyRouter);

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  const status = err.status || 500;
  const requestLogger = req.log || logger;

  const logData = {
    status: err.status,
    type: err.type || 'InternalServerError',
    message: err.message,
  };

  if (status >= 500 || process.env.NODE_ENV === 'development') {
    logData.stack = err.stack;
  }

  if (err.details) {
    logData.details = err.details;
  }

  if (status < 500) {
    requestLogger.warn(logData, 'Client error');
  } else {
    requestLogger.error(logData, 'Server error');
  }

  return res.status(status).json({
    error: err.type || 'InternalServerError',
    description: err.message,
  });
});

if (process.env.NODE_ENV !== 'test') {
  startup()
    .then(() => {
      if (config.roles.apiServer) {
        app.listen(PORT, () => {
          logger.debug(`API Server listening at port ${PORT}`);
        });
      }

      if (config.roles.fetcher) {
        startFetcher().catch((err) => {
          logger.error('[Fetcher] Failed to start', err);
          process.exit(1);
        });
      }
    })
    .catch((err) => {
      logger.error(`Startup failed: ${err}`);
      process.exit(1);
    });

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

async function startup() {
  if (!config.roles.apiServer && !config.roles.fetcher) {
    throw new Error('At least one FDA role must be enabled');
  }

  await createIndex();
  initLogger(config);

  if (config.roles.apiServer || config.roles.fetcher) {
    await initAgenda();
  }

  if (config.roles.apiServer) {
    try {
      if (fs.existsSync(UPLOAD_TMP_DIR)) {
        logger.debug(`[INIT] Upload temp directory exists: ${UPLOAD_TMP_DIR}`);
      } else {
        ensureUploadTmpDir();
        logger.info(`[INIT] Created upload temp directory: ${UPLOAD_TMP_DIR}`);
      }
    } catch (err) {
      logger.error(
        `[INIT] Failed to create upload temp directory: ${err.message}`,
      );
      throw err;
    }
    uploadTmpCleanupTimer = setInterval(() => {
      try {
        const files = fs.readdirSync(UPLOAD_TMP_DIR);
        const now = Date.now();
        const maxAge = 12 * 60 * 60 * 1000; // 12 hours

        files.forEach((file) => {
          const filePath = path.join(UPLOAD_TMP_DIR, file);
          const stats = fs.statSync(filePath);
          if (now - stats.mtimeMs > maxAge) {
            fs.unlinkSync(filePath);
            logger.debug(`[CLEANUP] Removed old temp file: ${file}`);
          }
        });
      } catch (err) {
        logger.warn('[CLEANUP] Failed to clean temp directory', err);
      }
    }, 3600000); // Run cleanup every hour
    uploadTmpCleanupTimer.unref?.();
  }

  getInitialLogger(config).fatal('[INIT]: Initializing app');
}

let shuttingDown = false;
let uploadTmpCleanupTimer;
async function shutdown() {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  logger.info('[SHUTDOWN] Graceful shutdown started');

  try {
    if (uploadTmpCleanupTimer) {
      clearInterval(uploadTmpCleanupTimer);
      uploadTmpCleanupTimer = undefined;
    }

    if (config.roles.fetcher) {
      await shutdownAgenda();
    }

    await disconnectClient();
    await destroyS3Client();
    await closePgPools();

    logger.info('[SHUTDOWN] Completed');
    process.exit(0);
  } catch (err) {
    logger.error('[SHUTDOWN] Failed', err);
    process.exit(1);
  }
}
