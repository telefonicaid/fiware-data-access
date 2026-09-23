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

// Pure helpers describing the shape of a MongoDB FDA query

function isMongoProjection(projection) {
  return (
    projection && typeof projection === 'object' && !Array.isArray(projection)
  );
}

function getMongoProjectionColumns(projection) {
  return Object.keys(projection).filter((column) => projection[column]);
}

function getMongoAggregationDeclaredColumns(aggregation) {
  if (!Array.isArray(aggregation) || aggregation.length === 0) {
    return [];
  }

  const finalStage = aggregation.at(-1);

  if (isMongoProjection(finalStage?.$project)) {
    return getMongoProjectionColumns(finalStage.$project);
  }

  // Every key of a $group stage is an output field, including `_id`: unlike in a
  // projection, its value is the grouping key, not an include/exclude flag.
  if (isMongoProjection(finalStage?.$group)) {
    return Object.keys(finalStage.$group);
  }

  return [];
}

/**
 * Output columns explicitly declared by the query itself: the `projection` of a
 * `find` query or the final `$project`/`$group` stage of an aggregation. They are
 * the only deterministic column contract available for a schemaless datasource,
 * since the CSV header must be fixed before the cursor is read.
 *
 * Returns an empty array when the query does not declare its output columns (no
 * projection, exclusion-only projection, or an aggregation not ending in a shaping
 * stage), in which case the column set has to be sampled from the first document.
 */
export function getMongoDeclaredColumns(query) {
  const { projection, aggregation } = query ?? {};

  if (Array.isArray(aggregation)) {
    return getMongoAggregationDeclaredColumns(aggregation);
  }

  return isMongoProjection(projection)
    ? getMongoProjectionColumns(projection)
    : [];
}
