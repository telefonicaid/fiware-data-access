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

import { createReadStream } from 'fs';
import { to as copyTo } from 'pg-copy-streams';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { getPgClient } from './pg.js';
import { getS3Client, newUpload } from './aws.js';

import { getDBConnection, toParquet } from './db.js';

const toNDJSON = new Transform({
  objectMode: true,
  transform(row, encoding, callback) {
    callback(null, JSON.stringify(row) + '\n');
  },
});

const nullSink = new (class NullWritable extends Transform {
  _transform(_chunk, _enc, cb) {
    cb(); // discard immediately
  }
})();

export async function storeSetPerformance(bucket, database, table, fda) {
  await pgToNode(bucket, database, table, fda);
  await nodeToMinio25MBChunk1Parallel(bucket, database, table, fda);
  await nodeToMinio5MBChunk1Parallel(bucket, database, table, fda);
  await nodeToMinio25MBChunk4Parallel(bucket, database, table, fda);
  await changeFormat(bucket, database, table, fda);
}

async function pgToNode(bucket, database, table, fda) {
  const pgClient = getPgClient(
    'fakseUser',
    'fakePassword',
    'fakeHost',
    5432,
    database
  );
  pgClient.connect();

  console.log('#=======Starting pgToNode=======#');
  console.time('pgToNode');

  const sanitizedTable = table.replace(/[^a-zA-Z0-9_]/g, '');
  const sanitizedDatabase = database.replace(/[^a-zA-Z0-9_]/g, '');
  const pgStream = pgClient.query(
    copyTo(
      `COPY ${sanitizedDatabase}.${sanitizedTable} TO STDOUT WITH CSV HEADER`
    )
  );

  try {
    await Promise.all([pipeline(pgStream, nullSink)]);
    console.timeEnd('pgToNode');
    console.log(' ');
  } catch (e) {
    pgStream.destroy(e);
    console.log('Error uploading:', e);
  } finally {
    pgStream.destroy();
    await pgClient.end();
  }
}

async function nodeToMinio25MBChunk1Parallel(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinio=======#');
  console.time('nodeToMinio');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = newUpload(
    getS3Client('http://localhost:9000', 'admin', 'admin123'),
    bucket,
    `${fda}.csv`,
    pgStream,
    25,
    1
  );

  try {
    await Promise.all([parallelUploads3.done()]);
    console.timeEnd('nodeToMinio');
    console.log(' ');
  } catch (e) {
    localStream.destroy(e);
    console.log('Error uploading:', e);
  } finally {
    localStream.destroy();
  }
}

async function nodeToMinio25MBChunk4Parallel(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinioQueueSize=======#');
  console.time('nodeToMinioQueueSize');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = newUpload(
    getS3Client('http://localhost:9000', 'admin', 'admin123'),
    bucket,
    `${fda}.csv`,
    pgStream,
    25,
    4
  );

  try {
    await Promise.all([parallelUploads3.done()]);
    console.timeEnd('nodeToMinioQueueSize');
    console.log(' ');
  } catch (e) {
    localStream.destroy(e);
    console.log('Error uploading:', e);
  } finally {
    localStream.destroy();
  }
}

async function nodeToMinio5MBChunk1Parallel(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinioPartSize=======#');
  console.time('nodeToMinioPartSize');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = newUpload(
    getS3Client('http://localhost:9000', 'admin', 'admin123'),
    bucket,
    `${fda}.csv`,
    pgStream,
    5,
    1
  );

  try {
    await Promise.all([parallelUploads3.done()]);
    console.timeEnd('nodeToMinioPartSize');
    console.log(' ');
  } catch (e) {
    localStream.destroy(e);
    console.log('Error uploading:', e);
  } finally {
    localStream.destroy();
  }
}

async function changeFormat(bucket, database, table, fda) {
  console.log('#=======Starting changeFormat=======#');
  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');

  console.time('changeFormat');
  await toParquet(
    conn,
    getPath(bucket, fda, 'csv'),
    getPath(bucket, fda, 'parquet')
  );
  console.timeEnd('changeFormat');
  console.log(' ');
}

const getPath = (path, fda, format) => {
  return `${path}${fda}.${format}`;
};
