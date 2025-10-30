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

export async function pocApi(params) {
  // Create DB in memory
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

  // Install and load  HTTP/S3 support
  await conn.run('INSTALL httpfs;');
  await conn.run('LOAD httpfs;');

  // Config access to MinIO (S3-compatible)
  await conn.run(`
    SET s3_endpoint='localhost:9000';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='admin123';
  `);

  const tableName = 'pocTable';
  const parquetPath = `s3://my-bucket-api/output/${tableName}.parquet`;

  await createTable(conn, tableName);
  await saveToMinIO(conn, tableName, parquetPath);
  const queryRes = await executeQueryWithResult(conn, parquetPath, params);

  conn.disconnectSync();
  return queryRes;
}

// Write parquet in MinIO
async function saveToMinIO(conn, tableName, parquetPath) {
  console.log(` Parquet file saved into MinIO: ${parquetPath}`);
  return conn.run(`
    COPY '${tableName}' TO '${parquetPath}' (FORMAT 'parquet');
  `);
}

// Create table from JSON
async function createTable(conn, tableName) {
  console.log(`Tabla '${tableName}' creada en DuckDB`);
  return conn.run(`
    CREATE OR REPLACE TABLE ${tableName} AS SELECT *
    FROM 'src/${tableName}.json';
  `);
}

// execute query and return promise
async function executeQueryWithResult(conn, parquetPath, params) {
  const result = await conn.run(
    `SELECT * FROM '${parquetPath}' WHERE ${params}`
  );
  return result.getRowObjectsJson();
}
