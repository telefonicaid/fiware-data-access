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
import { describe, test, expect } from '@jest/globals';
import ExcelJS from 'exceljs';
import { rowsToCsv, rowsToXlsx } from '../../src/lib/utils/outputFormat.js';

const rows = [
  { id: 1, big_value: JSON.rawJSON('9007199254740993'), amount: '12.34' },
  { id: 2, big_value: 7, amount: null },
];

describe('outputFormat', () => {
  test('rowsToCsv writes exact JSON numbers with all their digits', () => {
    expect(rowsToCsv(rows)).toBe(
      'id,big_value,amount\n1,9007199254740993,12.34\n2,7,',
    );
  });

  test('rowsToXlsx writes exact JSON numbers beyond the safe range as text with all their digits', async () => {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(await rowsToXlsx(rows));
    const sheet = workbook.getWorksheet('Results');

    expect(sheet.getRow(2).getCell(2).value).toBe('9007199254740993');
    expect(sheet.getRow(3).getCell(2).value).toBe(7);
  });
});
