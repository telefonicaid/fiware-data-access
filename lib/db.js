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
import { getFDA } from './mongo.js';

let instance = null;
let preparedStatements = new Map();

let connectionPool = [];
const MAX_POOL_SIZE = 10;

export async function releaseDBConnection(conn) {
  if (connectionPool.length < MAX_POOL_SIZE) {
    // Reset connection before return to pool
    try {
      await conn.run('RESET ALL;');
      connectionPool.push(conn);
    } catch (error) {
      // If error close connection
      await closeConnection(conn);
    }
  } else {
    // Pool full, close connection
    await closeConnection(conn);
  }
}

export async function getDBConnection() {
  await initDuckDB();

  // Reuse connection from pool if exists
  if (connectionPool.length > 0) {
    return connectionPool.pop();
  }

  // Create a new connection if pool is empty
  const conn = await instance.connect();
  return conn;
}

export async function runPreparedStatement(conn, setId, id, params) {
  if (!getPreparedStatement(setId, id)) {
    const fda = await getFDA(setId, id);
    if (!fda?.query) throw `FDA ${id} does not exist in set ${setId}.`;
    await storePreparedStatement(conn, setId, id, fda.query);
  }

  const dbStatement = getPreparedStatement(setId, id);
  dbStatement.bind(params);
  const result = await dbStatement.run();

  return result.getRowObjectsJson();
}

export async function toParquet(conn, originPath, resultPath) {
  return conn.run(
    `COPY ( SELECT * FROM read_csv_auto('s3://${originPath}')) 
    TO 's3://${resultPath}' (FORMAT PARQUET);`
  );
}

async function initDuckDB() {
  if (!instance) {
    console.log('Initializing DuckDB global instance...');
    instance = await DuckDBInstance.create(':memory:');

    // Init connection for config
    const configConn = await instance.connect();
    await configConn.run('INSTALL httpfs;');
    await configConn.run('LOAD httpfs;');

    // Common config
    await configConn.run(`
      SET s3_endpoint='localhost:9000';
      SET s3_url_style='path';
      SET s3_use_ssl=false;
      SET s3_access_key_id='admin';
      SET s3_secret_access_key='admin123';
    `);

    if (typeof configConn.disconnect === 'function') {
      await configConn.disconnect();
    }
    console.log('DuckDB initialized with HTTPFS');
  }
  return instance;
}

export function getPreparedStatement(setId, id) {
  return preparedStatements.get(setId)?.get(id);
}

export async function storePreparedStatement(conn, setId, id, query) {
  const dbStatement = await conn.prepare(query);

  const set = preparedStatements.get(setId);
  if (set) {
    set.set(id, dbStatement);
  } else {
    const fda = new Map();
    fda.set(id, dbStatement);
    preparedStatements.set(setId, fda);
  }
}

async function closeConnection(conn) {
  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }
}

export async function disconnectConnection() {
  // Close all pool connections
  for (const conn of connectionPool) {
    await closeConnection(conn);
  }
  connectionPool = [];

  if (instance) {
    // Close DuckDB instance
    instance = null;
  }
}
