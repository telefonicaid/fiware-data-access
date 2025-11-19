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

import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { PassThrough } from 'stream';

const { Client } = pg;

function getPgClient(user, password, host, port, database) {
  return new Client({
    user: user,
    password: password,
    host: host,
    port: port,
    database: database,
  });
}

function getS3Client(endpoint, user, password) {
  return new S3Client({
    endpoint: endpoint,
    region: 'REGION',
    credentials: {
      accessKeyId: user,
      secretAccessKey: password,
    },
    forcePathStyle: true,
  });
}

const toNDJSON = new Transform({
  objectMode: true,
  transform(row, encoding, callback) {
    callback(null, JSON.stringify(row) + '\n');
  },
});

export async function queryPG(bucket, database, table, fda) {
  const pgClient = getPgClient(
    'fakeUser',
    'fakePass',
    'fakeHost',
    5432,
    database
  );
  pgClient.connect();

  const sanitizedTable = table.replace(/[^a-zA-Z0-9_]/g, '');
  const sanitizedDatabase = database.replace(/[^a-zA-Z0-9_]/g, '');
  const query = new QueryStream(
    `SELECT * FROM ${sanitizedDatabase}.${sanitizedTable}`
  );
  const pgStream = pgClient.query(query);

  const pass = new PassThrough();
  pass.on('drain', () => {
    console.log('Backpressure: S3 exahust');
  });

  const parallelUploads3 = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.json`,
      Body: pass,
    },
    partSize: 25 * 1024 * 1024,
  });

  parallelUploads3.on('httpUploadProgress', (progress) => {
    console.log(progress);
  });

  try {
    await Promise.all([
      pipeline(pgStream, toNDJSON, pass),
      parallelUploads3.done(),
    ]);
    console.log('Upload completed successfully');
  } catch (e) {
    pass.destroy(e);
    pgStream.destroy(e);
    console.log('Error uploading:', e);
  } finally {
    pgStream.destroy();
    await pgClient.end();
  }
}
