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

import {
  runPreparedStatement,
  getDBConnection,
  toParquet,
  storePreparedStatement,
} from './db.js';
import { uploadSet } from './pg.js';
import { getS3Client, dropSet } from './aws.js';
import { storeSet as mongoStoreSet } from './mongo.js';

export async function querySet({ path, ...params }) {
  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');

  const queryRes = await runPreparedStatement(conn, path, params);

  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }

  return queryRes;
}

export async function storeSet(bucket, fda, query) {
  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');
  const parquetPath = getPath(bucket, fda, '');
  mongoStoreSet(parquetPath, query);
  storePreparedStatement(conn, parquetPath, query);
}

export async function fetchSet(database, table, bucket, fda) {
  const s3Client = await getS3Client(
    'http://localhost:9000',
    'admin',
    'admin123'
  );
  await uploadSet(s3Client, bucket, database, table, fda);

  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');
  const parquetPath = getPath(bucket, fda, '');
  await toParquet(conn, getPath(bucket, fda, '.csv'), parquetPath);
  await dropSet(s3Client, bucket, `${fda}.csv`);
}

const getPath = (path, fda, extension) => {
  return `${path}${fda}${extension}`;
};
