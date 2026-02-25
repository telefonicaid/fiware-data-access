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

import { executeQuery } from '../fda';

export async function handleCdaQuery(req) {
  const adaptedParams = adaptCdaParams(req.body);
  const rows = await executeQuery(adaptedParams);
  return adaptToCdaFormat(rows, adaptedParams);
}

function adaptCdaParams(body) {
  const { path, dataAccessId, pageSize, pageStart, ...rest } = body;

  const adapted = {};

  for (const [key, value] of Object.entries(rest)) {
    if (key.startsWith('param')) {
      const cleanKey = key.replace(/^param_not_/, '').replace(/^param/, '');

      // TODO: still need to handle "not" logic in the query execution, but for now just clean the key and pass the value as is
      adapted[cleanKey] = value;
    }
  }

  if (pageSize !== undefined) {
    adapted.limit = Number(pageSize);
  }

  if (pageStart !== undefined) {
    adapted.offset = Number(pageStart);
  }

  return adapted;
}

function adaptToCdaFormat(rows, { pageStart = 0, pageSize = 0 }) {
  if (!rows.length) {
    return {
      metadata: [],
      resultset: [],
      queryInfo: {
        pageStart,
        pageSize,
        totalRows: 0,
      },
    };
  }

  const columns = Object.keys(rows[0]);

  const metadata = columns.map((colName, index) => ({
    colIndex: index,
    colName,
  }));

  const resultset = rows.map((row) => columns.map((col) => row[col]));

  return {
    metadata,
    resultset,
    queryInfo: {
      pageStart,
      pageSize,
      totalRows: resultset.length,
    },
  };
}
