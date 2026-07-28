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

import { describe, expect, test } from '@jest/globals';
import xlsx from 'xlsx';

export function registerUploadFdasIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpMultipartReq,
  waitUntilFDACompleted,
}) {
  describe('Upload FDAs', () => {
    async function deleteFdaIfPresent(baseUrl, fdaId) {
      const res = await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      expect([204, 404]).toContain(res.status);
    }

    test('POST /{visibility}/fdas/upload creates a CSV FDA and defaultDataAccess is queryable', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = `upload_csv_${Date.now()}`;
      const csvBuffer = Buffer.from(
        'device,status,reading\nsensor-a,ok,21.5\nsensor-b,warn,19.2\n',
      );

      try {
        const uploadRes = await httpMultipartReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/upload`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          fields: {
            id: fdaId,
            description: 'integration csv upload',
          },
          file: {
            fieldName: 'file',
            filename: 'upload.csv',
            contentType: 'text/csv',
            content: csvBuffer,
          },
        });

        expect(uploadRes.status).toBe(202);
        expect(uploadRes.json).toEqual({ id: fdaId, status: 'pending' });

        const completed = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          visibility,
        });

        expect(completed.status).toBe('completed');
        expect(completed.datasourceId).toBe('upload');
        expect(completed.validationMode).toBe('strict');

        const dataRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/defaultDataAccess/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(dataRes.status).toBe(200);
        expect(Array.isArray(dataRes.json)).toBe(true);
        expect(dataRes.json.length).toBe(2);
      } finally {
        await deleteFdaIfPresent(baseUrl, fdaId);
      }
    });

    test('POST /{visibility}/fdas/upload supports XLSX multi-sheet uploads', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = `upload_xlsx_${Date.now()}`;

      const workbook = xlsx.utils.book_new();
      const sheetA = xlsx.utils.aoa_to_sheet([
        ['device', 'reading'],
        ['sensor-a', 10],
      ]);
      const sheetB = xlsx.utils.aoa_to_sheet([
        ['id', 'value'],
        ['b', 20],
      ]);
      xlsx.utils.book_append_sheet(workbook, sheetA, 'Main');
      xlsx.utils.book_append_sheet(workbook, sheetB, 'Other');
      const xlsxBuffer = xlsx.write(workbook, {
        type: 'buffer',
        bookType: 'xlsx',
      });

      try {
        const uploadRes = await httpMultipartReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/upload`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          fields: {
            id: fdaId,
            description: 'integration xlsx upload',
          },
          file: {
            fieldName: 'file',
            filename: 'upload.xlsx',
            contentType:
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            content: xlsxBuffer,
          },
        });

        expect(uploadRes.status).toBe(202);

        const completed = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          visibility,
        });

        expect(completed.status).toBe('completed');

        const dataRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/defaultDataAccess/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(dataRes.status).toBe(200);
        expect(Array.isArray(dataRes.json)).toBe(true);
        expect(dataRes.json.length).toBe(2);
        expect(dataRes.json[0]).toEqual(
          expect.objectContaining({
            __total: expect.any(String),
          }),
        );
      } finally {
        await deleteFdaIfPresent(baseUrl, fdaId);
      }
    });

    test('POST /{visibility}/fdas/upload with defaultDataAccess=false skips default DA creation', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = `upload_no_da_${Date.now()}`;
      const csvBuffer = Buffer.from('device,status\nsensor-a,ok\n');

      try {
        const uploadRes = await httpMultipartReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/upload`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          fields: {
            id: fdaId,
            defaultDataAccess: 'false',
          },
          file: {
            fieldName: 'file',
            filename: 'upload.csv',
            contentType: 'text/csv',
            content: csvBuffer,
          },
        });

        expect(uploadRes.status).toBe(202);

        const completed = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          visibility,
        });

        expect(completed.status).toBe('completed');
        expect(completed.das || {}).toEqual({});
      } finally {
        await deleteFdaIfPresent(baseUrl, fdaId);
      }
    });

    test('POST /{visibility}/fdas/upload rejects invalid partition synchronously', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = `upload_bad_partition_${Date.now()}`;
      const csvBuffer = Buffer.from(
        'device,status,reading\nsensor-a,ok,21.5\n',
      );

      const uploadRes = await httpMultipartReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/upload`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        fields: {
          id: fdaId,
          timeColumn: 'reading',
          objStgConf: JSON.stringify({ partition: 'invalid' }),
        },
        file: {
          fieldName: 'file',
          filename: 'upload.csv',
          contentType: 'text/csv',
          content: csvBuffer,
        },
      });

      expect(uploadRes.status).toBe(400);
      expect(uploadRes.json).toEqual({
        error: 'InvalidParam',
        description: 'Invalid partition type "invalid".',
      });
    });

    test('POST /{visibility}/fdas/upload marks FDA as failed when timeColumn does not exist in file', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = `upload_missing_timecolumn_${Date.now()}`;
      const csvBuffer = Buffer.from(
        'device,status,reading\nsensor-a,ok,21.5\n',
      );

      try {
        const uploadRes = await httpMultipartReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/upload`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          fields: {
            id: fdaId,
            timeColumn: 'event_date',
          },
          file: {
            fieldName: 'file',
            filename: 'upload.csv',
            contentType: 'text/csv',
            content: csvBuffer,
          },
        });

        expect(uploadRes.status).toBe(202);

        const finalState = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          visibility,
        });

        expect(finalState.status).toBe('failed');
        expect(finalState.error).toContain(
          'Column "event_date" not found in the uploaded file',
        );
      } finally {
        await deleteFdaIfPresent(baseUrl, fdaId);
      }
    });

    test('POST /{visibility}/fdas/upload rejects unsupported file type', async () => {
      const baseUrl = getBaseUrl();
      const uploadRes = await httpMultipartReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/upload`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        fields: {
          id: `upload_unsupported_${Date.now()}`,
        },
        file: {
          fieldName: 'file',
          filename: 'upload.txt',
          contentType: 'text/plain',
          content: Buffer.from('not tabular data'),
        },
      });

      expect(uploadRes.status).toBe(415);
      expect(uploadRes.json).toEqual({
        error: 'UnsupportedMediaType',
        description: 'Only CSV, XLS, or XLSX files are allowed',
      });
    });

    test('POST /{visibility}/fdas/upload rejects invalid FDA ids', async () => {
      const baseUrl = getBaseUrl();
      const uploadRes = await httpMultipartReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/upload`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        fields: {
          id: 'invalid_id!@#',
        },
        file: {
          fieldName: 'file',
          filename: 'upload.csv',
          contentType: 'text/csv',
          content: Buffer.from('device,status\nsensor-a,ok\n'),
        },
      });

      expect(uploadRes.status).toBe(400);
      expect(uploadRes.json).toEqual({
        error: 'InvalidParam',
        description:
          'FDA id must contain only alphanumeric characters, hyphens, and underscores.',
      });
    });
  });
}
