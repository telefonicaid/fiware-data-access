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

import { DuckDBInstance } from '@duckdb/node-api';

let instancePromise = null;

export async function getDuckDB() {
  if (!instancePromise) {
    instancePromise = initDuckDB();
  }
  return instancePromise;
}

export async function getDBConnection(endpoint, usr, pass) {
  const instance = await getDuckDB();
  const conn = await instance.connect();

  await conn.run(`
    SET s3_endpoint='${endpoint}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='${usr}';
    SET s3_secret_access_key='${pass}';
  `);
  return conn;
}

async function initDuckDB() {
  console.log(' Initializing DuckDB global instance...');

  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  await conn.run('INSTALL httpfs;');
  await conn.run('LOAD httpfs;');

  console.log(' HTTPFS extension loaded.');

  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }
  return instance;
}
