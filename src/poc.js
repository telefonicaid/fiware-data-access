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

import { getDBConnection } from './db.js';

export async function getFda(path, fda, colums) {
  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');

  const minioPath = getMinioPath(path, fda);

  const queryRes = await executeQueryWithResult(conn, minioPath, colums);

  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }

  return queryRes;
}

export async function storeSet(path, fda) {
  const conn = await getDBConnection('localhost:9000', 'admin', 'admin123');

  const minioPath = getMinioPath(path, fda);

  await createTable(conn, fda);
  await saveToMinIO(conn, fda, minioPath);

  if (typeof conn.disconnect === 'function') {
    await conn.disconnect();
  } else if (typeof conn.disconnectSync === 'function') {
    conn.disconnectSync();
  }
}

async function saveToMinIO(conn, tableName, parquetPath) {
  console.log(`Saving parquet file into MinIO: ${parquetPath}`);
  return conn.run(`
    COPY '${tableName}' TO '${parquetPath}' (FORMAT 'parquet');
  `);
}

async function createTable(conn, tableName) {
  console.log(`Creating table '${tableName}' in DuckDB...`);
  return conn.run(`
    CREATE OR REPLACE TABLE ${tableName} AS SELECT *
    FROM 'src/${tableName}.json';
  `);
}

async function executeQueryWithResult(conn, parquetPath, params) {
  const result = await conn.run(
    `SELECT * FROM '${parquetPath}' WHERE ${params}`
  );
  return result.getRowObjectsJson();
}

const getMinioPath = (path, fda) => {
  return `${path}${fda}.parquet`;
};
