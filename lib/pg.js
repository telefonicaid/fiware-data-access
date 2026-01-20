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
import { to as copyTo } from 'pg-copy-streams';
import { newUpload } from './aws.js';
import { config } from './fdaConfig.js';

const { Client } = pg;

function sanitizeString(string) {
  return string.replace(/[^a-zA-Z0-9_]/g, '');
}

export function getPgClient(user, password, host, port, database) {
  return new Client({
    user: user,
    password: password,
    host: host,
    port: port,
    database: database,
  });
}

export async function uploadTable(
  s3Client,
  bucket,
  database,
  schema,
  table,
  path
) {
  const pgClient = getPgClient(
    config.pg_client.usr,
    config.pg_client.pass,
    config.pg_client.host,
    config.pg_client.port,
    database
  );
  pgClient.connect();

  const pgStream = pgClient.query(
    copyTo(
      `COPY ${sanitizeString(database)}.${sanitizeString(
        schema
      )}.${sanitizeString(table)} TO STDOUT WITH CSV HEADER`
    )
  );

  const parallelUploads3 = newUpload(
    s3Client,
    bucket,
    `${path}.csv`,
    pgStream,
    25,
    1
  );

  parallelUploads3.on('httpUploadProgress', (progress) => {
    console.log(progress);
  });

  try {
    await parallelUploads3.done();
    console.log('Upload completed successfully');
  } catch (e) {
    pgStream.destroy(e);
    console.log('Error uploading:', e.message);
    throw e.message;
  } finally {
    pgStream.destroy();
    await pgClient.end();
  }
}
