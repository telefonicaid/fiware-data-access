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
import { createReadStream } from 'fs';
import { to as copyTo } from 'pg-copy-streams';
import { Transform, PassThrough } from 'stream';
import { pipeline } from 'stream/promises';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';

import { getDBConnection, toParquet } from './db.js';

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

const nullSink = new (class NullWritable extends Transform {
  _transform(_chunk, _enc, cb) {
    cb(); // discard immediately
  }
})();

export async function pgRemnant(bucket, database, table, fda) {
  console.log('#=======Starting pgRemnant=======#');
  console.time('pgRemnant');
  const pgClient = getPgClient(
    'postgres',
    'postgres',
    'localhost',
    5432,
    database
  );
  pgClient.connect();

  console.time('ndjson-stream');

  const sanitizedTable = table.replace(/[^a-zA-Z0-9_]/g, '');
  const sanitizedDatabase = database.replace(/[^a-zA-Z0-9_]/g, '');
  const pgStream = pgClient.query(
    copyTo(
      `COPY ${sanitizedDatabase}.${sanitizedTable} TO STDOUT WITH CSV HEADER`
    )
  );

  const pass = new PassThrough();

  let pgRows = 0;
  let drains = 0;
  pgStream.on('data', () => pgRows++);
  pass.on('drain', () => drains++);
  setInterval(() => {
    console.log({
      pgRowsPerSec: pgRows / 1,
      drainsPerSec: drains,
    });
    pgRows = 0;
    drains = 0;
  }, 1000);

  const parallelUploads3 = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.csv`,
      Body: pgStream,
    },
    partSize: 25 * 1024 * 1024,
  });

  parallelUploads3.on('httpUploadProgress', (progress) => {
    console.log(progress);
  });

  try {
    await Promise.all([
      //pipeline(pgStream, toNDJSON, pass),
      //pipeline(pgStream, nullSink),
      parallelUploads3.done(),
    ]);
    console.timeEnd('pgRemnant');
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

export async function performance(bucket, database, table, fda) {
  // await pgToNode(bucket, database, table, fda);
  // await nodeToMinio(bucket, database, table, fda);
  // await nodeToMinioPartSize(bucket, database, table, fda);
  // await nodeToMinioQueueSize(bucket, database, table, fda);
  // await changeFormat(bucket, database, table, fda);
  //await pgRemnant(bucket, database, table, fda);
  await pgToMinioWithBackpressure(bucket, database, table, fda);
}

async function pgToNode(bucket, database, table, fda) {
  const pgClient = getPgClient(
    'postgres',
    'postgres',
    'localhost',
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

async function nodeToMinio(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinio=======#');
  console.time('nodeToMinio');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.csv`,
      Body: localStream,
    },
    partSize: 25 * 1024 * 1024,
  });
  //parallelUploads3.on('httpUploadProgress', (progress) => {
  //  console.log(progress);
  //});

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

async function nodeToMinioQueueSize(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinioQueueSize=======#');
  console.time('nodeToMinioQueueSize');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.csv`,
      Body: localStream,
    },
    partSize: 25 * 1024 * 1024,
    queueSize: 4,
  });
  //parallelUploads3.on('httpUploadProgress', (progress) => {
  //  console.log(progress);
  //});

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

async function nodeToMinioPartSize(bucket, database, table, fda) {
  console.log('#=======Starting nodeToMinioPartSize=======#');
  console.time('nodeToMinioPartSize');

  const filePath = `./lib/${table}.csv`;
  const localStream = createReadStream(filePath);

  const parallelUploads3 = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.csv`,
      Body: localStream,
    },
    partSize: 5 * 1024 * 1024,
  });
  //parallelUploads3.on('httpUploadProgress', (progress) => {
  //  console.log(progress);
  //});

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

export async function pgToMinioWithBackpressure(bucket, database, table, fda) {
  console.log('#=======Starting pgToMinioWithBackpressure=======#');
  console.time('pgToMinioWithBackpressure');

  const pgClient = getPgClient(
    'postgres',
    'postgres',
    'localhost',
    5432,
    database
  );
  await pgClient.connect();

  const sanitizedTable = table.replace(/[^a-zA-Z0-9_]/g, '');
  const sanitizedDatabase = database.replace(/[^a-zA-Z0-9_]/g, '');
  const pgStream = pgClient.query(
    copyTo(
      `COPY ${sanitizedDatabase}.${sanitizedTable} TO STDOUT WITH CSV HEADER`
    )
  );

  const backpressure = new Transform({
    transform(chunk, enc, cb) {
      cb(null, chunk);
    },
  });

  const s3Body = new PassThrough();

  const pipePromise = pipeline(pgStream, backpressure, s3Body);

  const upload = new Upload({
    client: getS3Client('http://localhost:9000', 'admin', 'admin123'),
    params: {
      Bucket: bucket,
      Key: `${fda}.csv`,
      Body: s3Body,
    },
    partSize: 25 * 1024 * 1024,
  });

  try {
    await Promise.all([upload.done(), pipePromise]);

    console.timeEnd('pgToMinioWithBackpressure');
    console.log('Upload completed successfully\n');
  } catch (e) {
    console.error('Error uploading:', e);
    pgStream.destroy(e);
    s3Body.destroy(e);
  } finally {
    await pgClient.end();
  }
}

const getPath = (path, fda, format) => {
  return `${path}${fda}.${format}`;
};
