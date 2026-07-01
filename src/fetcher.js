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

import { getAgenda } from './lib/jobs.js';
import { cleanPartition, processFDAAsync } from './lib/fda.js';
import { getBasicLogger } from './lib/utils/logger.js';

const logger = getBasicLogger();

export async function startFetcher() {
  const agenda = getAgenda();

  const refreshFDA = async (job) => {
    const {
      fdaId,
      query,
      service,
      servicePath,
      timeColumn,
      refreshPolicy,
      objStgConf,
      datasourceId,
    } = job.attrs.data;
    // Should agenda also log errors?
    try {
      await processFDAAsync(
        fdaId,
        query,
        service,
        servicePath,
        timeColumn,
        refreshPolicy,
        objStgConf,
        datasourceId,
      );
    } catch (e) {
      logger.error('Fetcher error: ', e);
    }
  };

  const cleanPartitionFDA = async (job) => {
    const { fdaId, service, servicePath, windowSize, objStgConf } =
      job.attrs.data;
    try {
      await cleanPartition(service, fdaId, windowSize, objStgConf, servicePath);
    } catch (e) {
      logger.error('Fetcher error: ', e);
    }
  };

  agenda.define('refresh-fda', refreshFDA);
  agenda.define('refresh-fda-recurring', refreshFDA);
  agenda.define('clean-partition', cleanPartitionFDA);
  agenda.define('clean-partition-recurring', cleanPartitionFDA);

  await agenda.start();
  logger.info('[Fetcher] Agenda started');
}
