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

import {
  S3Client,
  CreateBucketCommand,
  DeleteObjectCommand,
  HeadBucketCommand,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { FDAError } from './fdaError.js';

let s3ClientInstance = null;

export function getS3Client(endpoint, user, password) {
  if (!s3ClientInstance) {
    s3ClientInstance = new S3Client({
      endpoint,
      region: 'REGION',
      credentials: {
        accessKeyId: user,
        secretAccessKey: password,
      },
      forcePathStyle: true,
    });
  }
  return s3ClientInstance;
}

export async function destroyS3Client() {
  if (s3ClientInstance) {
    await s3ClientInstance.destroy();
    s3ClientInstance = null;
  }
}

export function newUpload(client, bucket, path, body, partSize, queueSize) {
  return new Upload({
    client,
    params: {
      Bucket: bucket,
      Key: path,
      Body: body,
    },
    partSize: partSize * 1024 * 1024,
    queueSize,
  });
}

export async function dropFile(s3Client, bucket, path) {
  try {
    await s3Client.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: path,
      })
    );
  } catch (e) {
    throw new FDAError(
      500,
      'S3ServerError',
      `Error deleting file ${path} in bucket ${bucket}: ${e}`
    );
  }
}

export async function createBucket(s3Client, bucket) {
  try {
    await s3Client.send(new HeadBucketCommand({ Bucket: bucket }));
    console.log(`Bucket "${bucket}" already exists.`);
  } catch (err) {
    if (err.$metadata && err.$metadata.httpStatusCode === 404) {
      console.log(`Bucket "${bucket}" not found. Creating...`);

      await s3Client.send(new CreateBucketCommand({ Bucket: bucket }));
      console.log(`Bucket "${bucket}" created.`);
    } else {
      console.error('Unexpected error checking bucket:', err);
      throw err;
    }
  }
}
