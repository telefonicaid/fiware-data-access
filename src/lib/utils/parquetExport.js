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

import { FDAError } from '../fdaError.js';

export function getPartitionConf(timeColumn, partitionType = 'none') {
  if (!timeColumn && partitionType !== 'none') {
    throw new FDAError(400, 'PartitionError', `Missing timeColumn value.`);
  }
  const PARTITIONS = {
    day: {
      columns: `
      year(${timeColumn}::TIMESTAMP) as year,
      month(${timeColumn}::TIMESTAMP) as month,
      day(${timeColumn}::TIMESTAMP) as day
    `,
      partitionBy: 'year, month, day',
    },
    week: {
      columns: `
      year(${timeColumn}::TIMESTAMP) as year,
      strftime(${timeColumn}::TIMESTAMP, '%Y-%W') as week
      `,
      partitionBy: 'year, week',
    },
    month: {
      columns: `
      year(${timeColumn}::TIMESTAMP) as year,
      month(${timeColumn}::TIMESTAMP) as month
    `,
      partitionBy: 'year, month',
    },
    year: {
      columns: `
      year(${timeColumn}::TIMESTAMP) as year
    `,
      partitionBy: 'year',
    },
    none: {
      columns: '',
      partitionBy: '',
    },
  };

  const partitionConf = PARTITIONS[partitionType];
  if (!partitionConf) {
    throw new FDAError(
      400,
      'PartitionError',
      `Invalid partition type: ${partitionType}.`,
    );
  }

  const cols = partitionConf.columns ? `*, ${partitionConf.columns}` : '*';
  const partitionBy = partitionConf.partitionBy
    ? `, PARTITION_BY (${partitionConf.partitionBy})`
    : '';
  const orderBy = partitionConf.partitionBy
    ? `ORDER BY ${timeColumn}::TIMESTAMP`
    : '';

  return { cols, partitionBy, orderBy };
}

export function buildParquetCopyQuery(
  sourceQuery,
  target,
  timeColumn,
  partitionType,
  compression,
  sortPartitions = true,
) {
  const { cols, partitionBy, orderBy } = getPartitionConf(
    timeColumn,
    partitionType,
  );
  const compressionString = compression ? `, COMPRESSION ZSTD` : '';

  return `COPY ( SELECT ${cols}
                FROM (${sourceQuery}) AS fda_source ${sortPartitions ? orderBy : ''})
      TO '${target}' (FORMAT PARQUET ${partitionBy} ${compressionString});`;
}
