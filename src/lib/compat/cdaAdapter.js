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

import { executeQuery } from '../fda.js';

export async function handleCdaQuery({ body }) {
  const { service, fdaId, daId, queryParams } = adaptCdaParams(body);

  const rows = await executeQuery({
    service,
    params: {
      fdaId,
      daId,
      ...queryParams,
    },
  });

  return adaptToCdaFormat(rows, queryParams);
}

function adaptCdaParams(body) {
  const { path, dataAccessId, pageSize, pageStart, ...rest } = body;

  // --------- RESOLVE SERVICE ----------
  const pathParts = path.split('/').filter(Boolean);
  const service = pathParts.length <= 1 ? pathParts[0] : pathParts[1];

  const fdaId = body.cda || dataAccessId;
  const daId = dataAccessId;

  // --------- BUILD QUERY PARAMS ----------
  const queryParams = {};

  for (const [key, value] of Object.entries(rest)) {
    if (key.startsWith('param')) {
      const cleanKey = key.replace(/^param_not_/, '').replace(/^param/, '');

      queryParams[cleanKey] = value;
    }
  }

  if (pageSize !== undefined) {
    queryParams.limit = Number(pageSize);
  }

  if (pageStart !== undefined) {
    queryParams.offset = Number(pageStart);
  }

  return {
    service,
    fdaId,
    daId,
    queryParams,
  };
}

function adaptToCdaFormat(rows, { offset = 0, limit = 0 }) {
  if (!rows.length) {
    return {
      metadata: [],
      resultset: [],
      queryInfo: {
        pageStart: offset,
        pageSize: limit,
        totalRows: 0,
      },
    };
  }

  const totalRows = rows[0].__total || rows.length;

  const cleanedRows = rows.map(({ __total, ...rest }) => rest);

  const columns = Object.keys(cleanedRows[0]);

  const metadata = columns.map((colName, index) => ({
    colIndex: index,
    colName,
  }));

  const resultset = cleanedRows.map((row) => columns.map((col) => row[col]));

  return {
    metadata,
    resultset,
    queryInfo: {
      pageStart: offset,
      pageSize: limit,
      totalRows,
    },
  };
}
