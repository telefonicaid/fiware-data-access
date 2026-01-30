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
import { config } from './fdaConfig.js';

const uri = config.mongo.uri;
const client = new MongoClient(uri);
let isConnected = false;

async function getCollection() {
  if (!isConnected) {
    await client.connect();
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
  console.log('MongoDB connection closed');
}

export async function createFDA(
  fdaId,
  database,
  query,
  path,
  service,
  description
) {
  const collection = await getCollection();
  try {
    await collection.insertOne({
      fdaId,
      database,
      query,
      path,
      das: {},
      service,
      ...(description && { description }),
    });
  } catch (e) {
    if (e.code === 11000) {
      console.log(`FDA with id ${fdaId} and ${service} already exists.`);
    } else {
      console.log('Error creating FDA:', e);
    }
  }
}

export async function storeDA(service, fdaId, daId, description, query) {
  const collection = await getCollection();
  try {
    await collection.updateOne(
      { service, fdaId },
      { $set: { [`das.${daId}`]: { description, query } } }
    );
  } catch (e) {
    console.log(
      `Error storing DA ${daId} in FDA ${fdaId} of service ${service}: ${e}`
    );
  }
}

export async function retrieveFDAs(service) {
  const collection = await getCollection();
  try {
    return collection.find({ service }).toArray();
  } catch (e) {
    console.log(`Error retrieving all the FDAs of service ${service}: ${e}`);
    throw e.message;
  }
}

export async function retrieveFDA(service, fdaId) {
  const collection = await getCollection();
  try {
    return collection.findOne({ service, fdaId });
  } catch (e) {
    console.log(`Error retrieving FDA ${fdaId} of service ${service}: ${e}`);
    throw e.message;
  }
}

export async function removeFDA(service, fdaId) {
  const collection = await getCollection();
  try {
    const result = await collection.deleteOne({ service, fdaId });
    if (result.deletedCount === 0) {
      return 404;
    }

    return 204;
  } catch (e) {
    console.log(`Error deleting FDA ${fdaId} of service ${service}: ${e}`);
    throw e.message;
  }
}

export async function retrieveDAs(service, fdaId) {
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
                },
              },
            },
          },
        },
      ])
      .toArray();

    return das[0]?.das ?? [];
  } catch (e) {
    console.log(
      `Error retrieving all the DAs of FDA ${fdaId} and service ${service}: ${e}`
    );
    throw e.message;
  }
}

export async function retrieveDA(service, fdaId, daId) {
  const collection = await getCollection();
  try {
    const result = await collection.findOne(
      { service, fdaId },
      { projection: { [`das.${daId}`]: 1, _id: 0 } }
    );
    return result?.das?.[daId] || null;
  } catch (e) {
    console.log(
      `Error retrieving getting DA of FDA ${fdaId} and service ${service}: ${e}`
    );
    throw e.message;
  }
}

export async function updateDA(
  service,
  fdaId,
  daId,
  newId,
  description,
  query
) {
  const collection = await getCollection();
  try {
    const filter = { service, fdaId };
    const update = {};

    if (daId !== newId) {
      update.$unset = { [`das.${daId}`]: '' };
    }
    update.$set = {
      [`das.${newId}`]: {
        description,
        query,
      },
    };
    await collection.updateOne(filter, update);
  } catch (e) {
    console.log(
      `Error updating DA ${daId} of FDA ${fdaId} and service ${service}: ${e}`
    );
  }
}

export async function removeDA(service, fdaId, daId) {
  const collection = await getCollection();
  try {
    const filter = { service, fdaId };
    const update = { $unset: { [`das.${daId}`]: '' } };
    await collection.updateOne(filter, update);
  } catch (e) {
    console.log(
      `Error removing DA ${daId} of FDA ${fdaId} and service ${service}: ${e}`
    );
  }
}
