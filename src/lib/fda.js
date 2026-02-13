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
  runPreparedStatementStream,
  getDBConnection,
  toParquet,
  storePreparedStatement,
} from './utils/db.js';
import { uploadTable } from './utils/pg.js';
import { getS3Client, dropFile } from './utils/aws.js';
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
} from './utils/mongo.js';
import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

export function getFDAs(service) {
  return retrieveFDAs(service);
}

export async function getFDA(service, fdaId) {
  const fda = await retrieveFDA(service, fdaId);
  if (!fda) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} not found in service ${service}`,
    );
  }

  return fda;
}

export async function query(service, { fdaId, daId, ...params }) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass,
  );

  const queryRes = await runPreparedStatement(
    conn,
    service,
    fdaId,
    daId,
    params,
  );
  return queryRes;
}

export async function queryStream(service, { fdaId, daId, ...params }) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass,
  );

  const stream = await runPreparedStatementStream(
    conn,
    service,
    fdaId,
    daId,
    params,
  );
  return stream;
}

export async function createDA(service, fdaId, daId, description, userQuery) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass,
  );
  const query = buildDAQuery(service, fdaId, userQuery);
  await storePreparedStatement(conn, service, fdaId, daId, query);
  storeDA(service, fdaId, daId, description, query); // TO DISCUSS: should we store the user query instead of the built query?
}

export async function fetchFDA(fdaId, query, service, description) {
  await uploadTableToObjStg(service, query, service, fdaId);
  await createFDA(fdaId, query, service, description);
}

export async function updateFDA(service, fdaId) {
  const { query } = await retrieveFDA(service, fdaId);
  await uploadTableToObjStg(service, query, service, fdaId);
}

export async function deleteFDA(service, fdaId) {
  const { _id } = (await retrieveFDA(service, fdaId)) ?? {};

  if (!service || !_id) {
    throw new FDAError(
      404,
      'FDANotFound',
      `FDA ${fdaId} of the service ${service} not found.`,
    );
  }
  const s3Client = await getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  await dropFile(s3Client, service, getPath('', fdaId, '.parquet'));
  await removeFDA(service, fdaId);
}

export function getDAs(service, fdaId) {
  return retrieveDAs(service, fdaId);
}

export async function getDA(service, fdaId, daId) {
  const da = await retrieveDA(service, fdaId, daId);
  if (da) {
    da.id = daId;
  } else {
    throw new FDAError(
      404,
      'DaNotFound',
      `DA ${daId} not found in FDA ${fdaId} and service ${service}.`,
    );
  }

  return da;
}

export async function putDA(
  service,
  fdaId,
  daId,
  newId,
  description,
  userQuery,
) {
  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass,
  );

  const query = buildDAQuery(service, fdaId, userQuery);

  await storePreparedStatement(conn, service, fdaId, newId, query);

  await updateDA(service, fdaId, daId, newId, description, query); // TO DISCUSS: should we store the user query instead of the built query?
}

export async function deleteDA(service, fdaId, daId) {
  await removeDA(service, fdaId, daId);
}

async function uploadTableToObjStg(database, query, bucket, path) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  await uploadTable(s3Client, bucket, database, query, path);

  const conn = await getDBConnection(
    config.objstg.endpoint,
    config.objstg.usr,
    config.objstg.pass,
  );
  const parquetPath = getPath(bucket, path, '.parquet');
  await toParquet(conn, getPath(bucket, path, '.csv'), parquetPath);
  await dropFile(s3Client, bucket, `${path}.csv`);
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};

function buildDAQuery(service, fdaId, userQuery) {
  if (!userQuery || typeof userQuery !== 'string') {
    throw new FDAError(400, 'BadRequest', 'Invalid DA query');
  }

  if (/^\s*from\b/i.test(userQuery)) {
    throw new FDAError(
      400,
      'InvalidDAQuery',
      'DA query must not include FROM clause at start. It is managed internally.',
    );
  }

  const trimmed = userQuery.trim();

  const parquetPath = `s3://${service}/${fdaId}.parquet`;

  return `FROM read_parquet('${parquetPath}') ${trimmed}`; // TO DISCUSS: implementation by adding FROM clause at the beginning of the query
}
