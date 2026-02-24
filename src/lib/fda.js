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

import { getAgenda } from './jobs.js';
import {
  runPreparedStatement,
  runPreparedStatementStream,
  storeCachedQuery,
  getDBConnection,
  releaseDBConnection,
  toParquet,
  buildDAQuery,
} from './utils/db.js';
import { uploadTable } from './utils/pg.js';
import { getS3Client, dropFile } from './utils/aws.js';
import {
  createFDAMongo,
  regenerateFDA,
  retrieveFDAs,
  retrieveFDA,
  storeDA,
  removeFDA,
  retrieveDAs,
  retrieveDA,
  updateDA,
  removeDA,
  updateFDAStatus,
} from './utils/mongo.js';
import { convertBigInt } from './utils/utils.js';
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

export async function executeQuery({ service, params }) {
  const { fdaId, daId, ...rest } = params;

  const conn = await getDBConnection();

  try {
    return await runPreparedStatement(conn, service, fdaId, daId, rest);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function executeQueryStream({ service, params, req, res }) {
  const { fdaId, daId, ...rest } = params;

  const conn = await getDBConnection();

  let stream;
  let close;

  try {
    const result = await runPreparedStatementStream(
      conn,
      service,
      fdaId,
      daId,
      rest,
    );

    stream = result.stream;
    close = result.close;
  } catch (err) {
    await releaseDBConnection(conn);
    throw err;
  }

  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) {
      return;
    }
    cleaned = true;

    try {
      await close();
    } finally {
      await releaseDBConnection(conn);
    }
  };

  req.on('close', () => {
    cleanup().catch(() => {});
  });

  res.setHeader('Content-Type', 'application/x-ndjson');

  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const chunk = await stream.fetchChunk();
      if (chunk.rowCount === 0) {
        break;
      }

      const rows = chunk.getRows();
      const columnNames = stream.columnNames();

      for (const row of rows) {
        const rowObj = {};

        for (let i = 0; i < columnNames.length; i++) {
          rowObj[columnNames[i]] = row[i];
        }

        const safeObj = convertBigInt(rowObj);

        const ok = res.write(JSON.stringify(safeObj) + '\n');
        if (!ok) {
          await new Promise((resolve) => res.once('drain', resolve));
        }
      }
    }
  } finally {
    await cleanup();
  }

  return res.end();
}

export async function createDA(
  service,
  fdaId,
  daId,
  description,
  userQuery,
  params,
) {
  const conn = await getDBConnection();
  try {
    const query = buildDAQuery(service, fdaId, userQuery);
    await storeCachedQuery(conn, service, fdaId, daId, query, params);
    storeDA(service, fdaId, daId, description, userQuery, params);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function fetchFDA(
  fdaId,
  query,
  service,
  servicePath,
  description,
) {
  await createFDAMongo(fdaId, query, service, servicePath, description);

  const agenda = getAgenda();

  await agenda.now('refresh-fda', {
    fdaId,
    query,
    service,
  });
}

export async function updateFDA(service, fdaId) {
  const previous = await regenerateFDA(service, fdaId);

  const agenda = getAgenda();

  await agenda.now('refresh-fda', {
    fdaId,
    query: previous.query,
    service,
  });
}

export async function processFDAAsync(fdaId, query, service) {
  try {
    await updateFDAStatus(service, fdaId, 'fetching', 10);

    await uploadTableToObjStg(service, service, query, service, fdaId);

    await updateFDAStatus(service, fdaId, 'completed', 100);
  } catch (err) {
    await updateFDAStatus(service, fdaId, 'failed', 0, err.message);
    throw err;
  }
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

export async function putDA(service, fdaId, daId, description, userQuery) {
  const conn = await getDBConnection();

  try {
    const query = buildDAQuery(service, fdaId, userQuery);
    await storeCachedQuery(conn, service, fdaId, daId, query);
    await updateDA(service, fdaId, daId, description, userQuery);
  } finally {
    await releaseDBConnection(conn);
  }
}

export async function deleteDA(service, fdaId, daId) {
  await removeDA(service, fdaId, daId);
}

async function uploadTableToObjStg(service, database, query, bucket, path) {
  const s3Client = getS3Client(
    `${config.objstg.protocol}://${config.objstg.endpoint}`,
    config.objstg.usr,
    config.objstg.pass,
  );
  await updateFDAStatus(service, path, 'fetching', 20);
  await uploadTable(s3Client, bucket, database, query, path);

  const conn = await getDBConnection();
  try {
    await updateFDAStatus(service, path, 'transforming', 60);
    const parquetPath = getPath(bucket, path, '.parquet');
    await toParquet(conn, getPath(bucket, path, '.csv'), parquetPath);
    await updateFDAStatus(service, path, 'uploading', 80);
    await dropFile(s3Client, bucket, `${path}.csv`);
  } finally {
    await releaseDBConnection(conn);
  }
}

const getPath = (bucket, path, extension) => {
  const cleanBucket = bucket?.endsWith('/') ? bucket.slice(0, -1) : bucket;
  const cleanPath = path?.startsWith('/') ? path.slice(1) : path;
  return `${cleanBucket}/${cleanPath}${extension}`;
};
