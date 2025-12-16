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

const uri = 'mongodb://root:example@localhost:27017/?authSource=admin';

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

export async function createFDA(fdaId, database, table, bucket, path, service) {
  const collection = await getCollection();
  try {
    await collection.insertOne({
      fdaId,
      database,
      table,
      bucket,
      path,
      queries: {},
      service,
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
      { $set: { [`queries.${daId}`]: { description, query } } }
    );
  } catch (e) {
    console.log(
      `Error storing DA ${daId} in FDA ${fdaId} of service ${service}: ${e}`
    );
  }
}

export async function retrieveFDAs(service) {
  const collection = await getCollection();
  return collection.find({ service }).toArray();
}

export async function retrieveFDA(service, fdaId) {
  const collection = await getCollection();
  return collection.findOne({ service, fdaId });
}

export async function getDA(service, fdaId, daId) {
  const collection = await getCollection();
  const result = await collection.findOne(
    { service, fdaId },
    { projection: { [`queries.${daId}`]: 1, _id: 0 } }
  );
  const da = result?.queries?.[daId] || null;
  return da;
}
