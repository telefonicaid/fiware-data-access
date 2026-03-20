// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefonica Soluciones
// de Informatica y Comunicaciones de Espana, S.A.U (hereinafter TSOL) and is protected
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

import { beforeEach, describe, expect, jest, test } from '@jest/globals';

const loggerMock = {
  debug: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
};

const s3ClientCtorMock = jest.fn();
const createBucketCommandCtorMock = jest.fn(
  function createBucketCommand(input) {
    this.input = input;
  },
);
const deleteObjectCommandCtorMock = jest.fn(
  function deleteObjectCommand(input) {
    this.input = input;
  },
);
const headBucketCommandCtorMock = jest.fn(function headBucketCommand(input) {
  this.input = input;
});
const uploadCtorMock = jest.fn();

let currentS3Client;

async function loadAwsModule() {
  jest.resetModules();

  loggerMock.debug.mockClear();
  loggerMock.info.mockClear();
  loggerMock.error.mockClear();
  s3ClientCtorMock.mockClear();
  createBucketCommandCtorMock.mockClear();
  deleteObjectCommandCtorMock.mockClear();
  headBucketCommandCtorMock.mockClear();
  uploadCtorMock.mockClear();

  currentS3Client = {
    send: jest.fn(),
    destroy: jest.fn().mockResolvedValue(undefined),
  };

  s3ClientCtorMock.mockImplementation(() => currentS3Client);

  await jest.unstable_mockModule('@aws-sdk/client-s3', () => ({
    S3Client: s3ClientCtorMock,
    CreateBucketCommand: createBucketCommandCtorMock,
    DeleteObjectCommand: deleteObjectCommandCtorMock,
    HeadBucketCommand: headBucketCommandCtorMock,
  }));

  await jest.unstable_mockModule('@aws-sdk/lib-storage', () => ({
    Upload: uploadCtorMock,
  }));

  await jest.unstable_mockModule('../../src/lib/utils/logger.js', () => ({
    getBasicLogger: () => loggerMock,
  }));

  return import('../../src/lib/utils/aws.js');
}

describe('aws utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('dropFile wraps delete errors with FDAError', async () => {
    const { dropFile } = await loadAwsModule();

    currentS3Client.send.mockRejectedValueOnce(new Error('delete exploded'));

    await expect(
      dropFile(currentS3Client, 'bucket-a', 'file-a'),
    ).rejects.toMatchObject({
      status: 500,
      type: 'S3ServerError',
    });
  });

  test('createBucket logs when bucket already exists', async () => {
    const { createBucket } = await loadAwsModule();

    currentS3Client.send.mockResolvedValueOnce({});

    await createBucket(currentS3Client, 'existing-bucket');

    expect(headBucketCommandCtorMock).toHaveBeenCalledWith({
      Bucket: 'existing-bucket',
    });
    expect(loggerMock.info).toHaveBeenCalledWith(
      'Bucket "existing-bucket" already exists.',
    );
  });

  test('createBucket creates bucket when HeadBucket returns 404', async () => {
    const { createBucket } = await loadAwsModule();

    currentS3Client.send
      .mockRejectedValueOnce({ $metadata: { httpStatusCode: 404 } })
      .mockResolvedValueOnce({});

    await createBucket(currentS3Client, 'new-bucket');

    expect(createBucketCommandCtorMock).toHaveBeenCalledWith({
      Bucket: 'new-bucket',
    });
    expect(loggerMock.info).toHaveBeenCalledWith(
      'Bucket "new-bucket" not found. Creating...',
    );
    expect(loggerMock.info).toHaveBeenCalledWith(
      'Bucket "new-bucket" created.',
    );
  });

  test('createBucket rethrows unexpected errors while logging', async () => {
    const { createBucket } = await loadAwsModule();

    const unexpected = new Error('network fail');
    currentS3Client.send.mockRejectedValueOnce(unexpected);

    await expect(createBucket(currentS3Client, 'bad-bucket')).rejects.toBe(
      unexpected,
    );

    expect(loggerMock.error).toHaveBeenCalledWith(
      'Unexpected error checking bucket:',
      unexpected,
    );
  });
});
