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

import multer from 'multer';
import fs from 'node:fs';
import path from 'node:path';

import { config } from './fdaConfig.js';
import { FDAError } from './fdaError.js';

export const UPLOAD_TMP_DIR = config.fileUpload?.tmpDir || '/tmp/fda_uploads';

export function ensureUploadTmpDir() {
  if (!fs.existsSync(UPLOAD_TMP_DIR)) {
    fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true });
  }
}

ensureUploadTmpDir();
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, UPLOAD_TMP_DIR);
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    const ext = path.extname(file.originalname);
    cb(null, file.fieldname + '-' + uniqueSuffix + ext);
  },
});

const originalRemoveFile = storage._removeFile?.bind(storage);
if (originalRemoveFile) {
  storage._removeFile = (req, file, cb) => {
    if (!file || typeof file.path !== 'string') {
      return cb(null);
    }

    return originalRemoveFile(req, file, cb);
  };
}

export const uploadMiddleware = multer({
  storage,
  limits: { fileSize: config.fileUpload?.maxSize || 50 * 1024 * 1024 }, // 50 MB by default
  fileFilter: (req, file, cb) => {
    const allowedMimes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ];
    const allowedExtensions = /\.(csv|xls|xlsx)$/i;
    if (
      allowedMimes.includes(file.mimetype) ||
      allowedExtensions.test(file.originalname)
    ) {
      cb(null, true);
    } else {
      cb(
        new FDAError(
          415,
          'UnsupportedMediaType',
          'Only CSV, XLS, or XLSX files are allowed',
        ),
        false,
      );
    }
  },
}).single('file'); // The form field must be named 'file'
