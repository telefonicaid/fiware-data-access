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

import ExcelJS from 'exceljs';

export const VALID_OUTPUT_TYPES = ['json', 'csv', 'xls'];
export const DEFAULT_OUTPUT_TYPE = 'json';

/**
 * Converts an array of row objects to a CSV string.
 * Values containing commas, double-quotes, or newlines are wrapped in double-quotes.
 * Embedded double-quotes are escaped by doubling them.
 */
export function rowsToCsv(rows) {
  if (!rows.length) {
    return '';
  }

  const columns = Object.keys(rows[0]);

  const escape = (v) => {
    const s = v === null || v === undefined ? '' : String(v);
    if (
      s.includes(',') ||
      s.includes('"') ||
      s.includes('\n') ||
      s.includes('\r')
    ) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };

  const header = columns.map(escape).join(',');
  const dataLines = rows.map((row) =>
    columns.map((col) => escape(row[col])).join(','),
  );

  return [header, ...dataLines].join('\n');
}

/**
 * Converts an array of row objects to an Excel (.xlsx) buffer.
 */
export function rowsToXlsx(rows) {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Results');

  if (rows.length > 0) {
    const columns = Object.keys(rows[0]);
    sheet.columns = columns.map((col) => ({
      header: col,
      key: col,
      width: 20,
    }));
    for (const row of rows) {
      sheet.addRow(row);
    }
  }

  return workbook.xlsx.writeBuffer();
}
