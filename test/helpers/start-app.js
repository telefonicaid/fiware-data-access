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

import http from 'node:http';

const port = process.env.FDA_SERVER_PORT || '0';

// Simple ping loop to wait for health
function waitFor(url, timeoutMs = 30_000) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      const req = http.get(url, (res) => {
        res.resume();
        if (res.statusCode && res.statusCode < 500) return resolve();
        if (Date.now() - start > timeoutMs)
          return reject(new Error('timeout waiting app'));
        setTimeout(tick, 200);
      });
      req.on('error', () => {
        if (Date.now() - start > timeoutMs)
          return reject(new Error('timeout waiting app'));
        setTimeout(tick, 200);
      });
    };
    tick();
  });
}

// Nothing else here. This file is only used as the entrypoint for node child process.
console.log(`[CHILD] starting app... FDA_SERVER_PORT=${port}`);
// Your real entrypoint:
await import('../../index.js');

// If your app does not listen in NODE_ENV=test, force it by NOT using NODE_ENV=test in child.
// We'll handle that from the test (set NODE_ENV=integration).
// Add a health route in app? If you don't have one, we'll just rely on /fdas 400.
