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

import { MongoClient } from 'mongodb';
import { config } from '../fdaConfig.js';
import { FDAError } from '../fdaError.js';
import { getBasicLogger } from './logger.js';

const uri = config.mongo.uri;
const client = new MongoClient(uri);
const logger = getBasicLogger();
let isConnected = false;

async function getCollection() {
  if (!isConnected) {
    try {
      await client.connect();
    } catch (e) {
      throw new FDAError(
        503,
        'MongoConnectionError',
        `Error connecting to MongoDB: ${e}`,
      );
    }
    isConnected = true;
  }
  const db = client.db('fiware-data-access');
  return db.collection('fdas');
}

export async function createIndex() {
  const collection = await getCollection();
  collection.createIndex({ fdaId: 1, service: 1 }, { unique: true });
}

export async function disconnectClient() {
  await client.close();
  logger.debug('MongoDB connection closed');
}

export async function createFDAMongo(
  fdaId,
  query,
  service,
  servicePath,
  description,
) {
  logger.debug({ fdaId, query, service, description }, '[DEBUG]: createFDA');
  const collection = await getCollection();
  try {
    // As there is a unique index on (fdaId, service), this will throw an error if an FDA with the same fdaId and service already exists
    await collection.insertOne({
      fdaId,
      query,
      das: {},
      service,
      status: 'fetching',
      progress: 0,
      lastFetch: new Date(),
      ...(servicePath && { servicePath }),
      ...(description && { description }),
    });
  } catch (e) {
    if (e.code === 11000) {
      throw new FDAError(
        409,
        'DuplicatedKey',
        `FDA with id ${fdaId} and ${service} already exists: ${e}`,
      );
    } else {
      throw new FDAError(
        500,
        'MongoDBServerError',
        `Error creating fda ${fdaId} in service ${service}: ${e}`,
      );
    }
  }
}

export async function updateFDAStatus(
  service,
  fdaId,
  status,
  progress,
  error = null,
) {
  const collection = await getCollection();

  await collection.updateOne(
    { service, fdaId },
    {
      $set: {
        status,
        progress,
        lastFetch: new Date(),
        ...(error && { error }),
      },
    },
  );
}

export async function regenerateFDA(service, fdaId) {
  const collection = await getCollection();

  const previous = await collection.findOneAndUpdate(
    {
      service,
      fdaId,
      status: { $in: ['completed', 'failed'] },
    },
    {
      $set: {
        status: 'fetching',
        progress: 0,
        lastFetch: new Date(),
      },
    },
    { returnDocument: 'before' },
  );

  if (!previous) {
    const existing = await collection.findOne({ service, fdaId });

    if (!existing) {
      throw new FDAError(
        404,
        'NotFound',
        `FDA ${fdaId} not found in service ${service}`,
      );
    }

    if (existing.status === 'fetching') {
      throw new FDAError(
        409,
        'AlreadyFetching',
        `FDA ${fdaId} is already being regenerated`,
      );
    }

    throw new FDAError(
      409,
      'InvalidState',
      `FDA ${fdaId} cannot be regenerated from status ${existing.status}`,
    );
  }

  return previous;
}

export async function storeDA(
  service,
  fdaId,
  daId,
  description,
  query,
  params,
) {
  logger.debug(
    { service, fdaId, daId, description, query },
    '[DEBUG]: storeDA',
  );
  const collection = await getCollection();
  try {
    await collection.updateOne(
      { service, fdaId },
      { $set: { [`das.${daId}`]: { description, query, params } } },
    );
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error storing DA ${daId} in FDA ${fdaId} of service ${service}: ${e}`,
    );
  }
}

export async function retrieveFDAs(service) {
  logger.debug({ service }, '[DEBUG]: retrieveFDAs');
  const collection = await getCollection();
  try {
    return collection.find({ service }).toArray();
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error retrieving all the FDAs of service ${service}: ${e.message}`,
    );
  }
}

export async function retrieveFDA(service, fdaId) {
  logger.debug({ service, fdaId }, '[DEBUG]: retrieveFDA');
  const collection = await getCollection();
  try {
    return collection.findOne({ service, fdaId });
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error retrieving FDA ${fdaId} of service ${service}: ${e}`,
    );
  }
}

export async function removeFDA(service, fdaId) {
  logger.debug({ service, fdaId }, '[DEBUG]: removeFDA');
  const collection = await getCollection();
  try {
    const result = await collection.deleteOne({ service, fdaId });
    if (result.deletedCount === 0) {
      throw new FDAError(
        404,
        'FDANotFound',
        `FDA ${fdaId} of the service ${service} not found.`,
      );
    }
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error deleting FDA ${fdaId} of service ${service}: ${e}`,
    );
  }
}

export async function retrieveDAs(service, fdaId) {
  logger.debug({ service, fdaId }, '[DEBUG]: retrieveDAs');
  const collection = await getCollection();
  try {
    const das = await collection
      .aggregate([
        { $match: { service, fdaId } },
        {
          $project: {
            _id: 0,
            das: {
              $map: {
                input: { $objectToArray: '$das' },
                as: 'q',
                in: {
                  id: '$$q.k',
                  description: '$$q.v.description',
                  query: '$$q.v.query',
                  params: '$$q.v.params',
                },
              },
            },
          },
        },
      ])
      .toArray();

    return das[0]?.das ?? [];
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error retrieving all the DAs of FDA ${fdaId} and service ${service}: ${e}`,
    );
  }
}

export async function retrieveDA(service, fdaId, daId) {
  logger.debug({ service, fdaId, daId }, '[DEBUG]: retrieveDA');
  const collection = await getCollection();
  try {
    const result = await collection.findOne(
      { service, fdaId },
      { projection: { [`das.${daId}`]: 1, _id: 0 } },
    );
    return result?.das?.[daId] || null;
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error retrieving getting DA of FDA ${fdaId} and service ${service}: ${e}`,
    );
  }
}

export async function updateDA(
  service,
  fdaId,
  daId,
  description,
  query,
  params,
) {
  logger.debug(
    { service, fdaId, daId, description, query, params },
    '[DEBUG]: updateDA',
  );
  const collection = await getCollection();

  try {
    const filter = { service, fdaId };

    const setFields = {};
    if (description !== undefined) {
      setFields[`das.${daId}.description`] = description;
    }

    if (query !== undefined) {
      setFields[`das.${daId}.query`] = query;
    }

    if (params !== undefined) {
      setFields[`das.${daId}.params`] = params;
    }

    if (Object.keys(setFields).length === 0) {
      return; // nothing to update
    }

    await collection.updateOne(filter, { $set: setFields });
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error updating DA ${daId} of FDA ${fdaId} and service ${service}: ${e}`,
    );
  }
}

export async function removeDA(service, fdaId, daId) {
  logger.debug({ service, fdaId, daId }, '[DEBUG]: removeDA');
  const collection = await getCollection();
  try {
    const filter = { service, fdaId };
    const update = { $unset: { [`das.${daId}`]: '' } };
    await collection.updateOne(filter, update);
  } catch (e) {
    throw new FDAError(
      500,
      'MongoDBServerError',
      `Error removing DA ${daId} of FDA ${fdaId} and service ${service}: ${e}`,
    );
  }
}
