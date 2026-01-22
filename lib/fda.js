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
import { uploadTable } from './pg.js';
import { getS3Client, dropFile } from './aws.js';
import {
  createFDA,
  retrieveFDAs,
  retrieveFDA,
  storeDA,
  removeFDA,
  retrieveDAs,
  retrieveDA,
  updateDA,
  removeDA,
} from './mongo.js';
import { config } from './fdaConfig.js';

export function getFDAs(service) {
  return retrieveFDAs(service);
}

export function getFDA(service, fdaId) {
  return retrieveFDA(service, fdaId);
}

export async function query(service, { fdaId, daId, ...params }) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass
  );

  const queryRes = await runPreparedStatement(
    conn,
    service,
    fdaId,
    daId,
    params
  );
  return queryRes;
}

export async function createDA(service, fdaId, daId, description, query) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass
  );
  storeDA(service, fdaId, daId, description, query);
  storePreparedStatement(conn, service, fdaId, daId, query);
}

export async function fetchFDA(
  fdaId,
  database,
  schema,
  table,
  query,
  path,
  service,
  description
) {
  await uploadTableToObjStg(database, schema, table, query, service, path);
  await createFDA(
    fdaId,
    database,
    schema,
    table,
    query,
    path,
    service,
    description
  );
}

export async function updateFDA(service, fdaId) {
  const { database, schema, table, path } = await retrieveFDA(service, fdaId);
  await uploadTableToObjStg(database, schema, table, service, path);
}

export async function deleteFDA(service, fdaId) {
  const { path } = (await retrieveFDA(service, fdaId)) ?? {};

  if (!service || !path) {
    return 404;
  }
  const s3Client = await getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass
  );
  await dropFile(s3Client, service, path);
  return await removeFDA(service, fdaId);
}

export function getDAs(service, fdaId) {
  return retrieveDAs(service, fdaId);
}

export async function getDA(service, fdaId, daId) {
  const da = await retrieveDA(service, fdaId, daId);
  if (da) {
    da.id = daId;
  }

  return da;
}

export async function putDA(service, fdaId, daId, newId, description, query) {
  await updateDA(service, fdaId, daId, newId, description, query);
}

export async function deleteDA(service, fdaId, daId) {
  await removeDA(service, fdaId, daId);
}

async function uploadTableToObjStg(
  database,
  schema,
  table,
  query,
  bucket,
  path
) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass
  );
  await uploadTable(s3Client, bucket, database, schema, table, query, path);

  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass
  );
  const parquetPath = getPath(bucket, path, '');
  await toParquet(conn, getPath(bucket, path, '.csv'), parquetPath);
  await dropFile(s3Client, bucket, `${path}.csv`);
}

const getPath = (bucket, path, extension) => {
  return `${bucket}${path}${extension}`;
};
