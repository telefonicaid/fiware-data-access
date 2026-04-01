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

import { config } from './fdaConfig.js';

const SERVICE_VERSION = process.env.npm_package_version || 'unknown';
const PROCESS_START_TIME_SECONDS = Math.floor(Date.now() / 1000);

const state = {
  totalRequests: 0,
  totalErrorRequests: 0,
  inFlightRequests: 0,
  httpRequestsByLabel: new Map(),
  httpDurationByLabel: new Map(),
  httpErrorsByLabel: new Map(),
  fiwareRequestsByLabel: new Map(),
  servicesObserved: new Set(),
  servicePathsObserved: new Set(),
  fiwareHeaderRequestsTotal: 0,
};

function nowMs() {
  return Date.now();
}

function escapeLabelValue(value) {
  return String(value)
    .replaceAll('\\', '\\\\')
    .replaceAll('\n', '\\n')
    .replaceAll('"', '\\"');
}

function labelsKey(labels) {
  const keys = Object.keys(labels).sort();
  return keys.map((key) => `${key}=${labels[key]}`).join('|');
}

function formatLabels(labels) {
  const keys = Object.keys(labels);
  if (keys.length === 0) {
    return '';
  }

  return `{${keys
    .sort()
    .map((key) => `${key}="${escapeLabelValue(labels[key])}"`)
    .join(',')}}`;
}

function getRouteLabel(req) {
  if (req.route && req.route.path) {
    const baseUrl = req.baseUrl || '';
    return `${baseUrl}${req.route.path}`;
  }

  return '__unmatched__';
}

function getStatusClass(statusCode) {
  const family = Math.floor(Number(statusCode || 0) / 100);
  return `${family}xx`;
}

function incrementCounter(map, labels, increment = 1) {
  const key = labelsKey(labels);
  const entry = map.get(key);

  if (entry) {
    entry.value += increment;
    return;
  }

  map.set(key, {
    labels,
    value: increment,
  });
}

function observeDuration(labels, durationMs) {
  const key = labelsKey(labels);
  const entry = state.httpDurationByLabel.get(key);

  if (entry) {
    entry.sumMs += durationMs;
    entry.count += 1;
    return;
  }

  state.httpDurationByLabel.set(key, {
    labels,
    sumMs: durationMs,
    count: 1,
  });
}

function getMetricsContentType(acceptHeader) {
  const accept = acceptHeader || '';

  if (accept.includes('application/openmetrics-text')) {
    return {
      ok: true,
      contentType: 'application/openmetrics-text; version=1.0.0; charset=utf-8',
    };
  }

  if (!accept || accept.includes('text/plain') || accept.includes('*/*')) {
    return {
      ok: true,
      contentType: 'text/plain; version=0.0.4; charset=utf-8',
    };
  }

  return {
    ok: false,
  };
}

function renderMetricLines(metricName, map) {
  return Array.from(map.values())
    .sort((a, b) => labelsKey(a.labels).localeCompare(labelsKey(b.labels)))
    .map(
      (entry) => `${metricName}${formatLabels(entry.labels)} ${entry.value}`,
    );
}

export function onRequestStart() {
  state.inFlightRequests += 1;
  return nowMs();
}

export function onRequestFinish(req, res, startMs) {
  const durationMs = Math.max(0, nowMs() - startMs);
  const method = req.method || 'UNKNOWN';
  const route = getRouteLabel(req);
  const statusCode = String(res.statusCode || 0);
  const statusClass = getStatusClass(res.statusCode || 0);

  state.totalRequests += 1;
  state.inFlightRequests = Math.max(0, state.inFlightRequests - 1);

  incrementCounter(state.httpRequestsByLabel, {
    method,
    route,
    status_code: statusCode,
    status_class: statusClass,
  });

  observeDuration(
    {
      method,
      route,
      status_class: statusClass,
    },
    durationMs,
  );

  if (res.statusCode >= 400) {
    state.totalErrorRequests += 1;
    incrementCounter(state.httpErrorsByLabel, {
      method,
      route,
      status_code: statusCode,
      status_class: statusClass,
    });
  }

  const fiwareService = req.get('Fiware-Service');
  const fiwareServicePath = req.get('Fiware-ServicePath');

  if (fiwareService && fiwareServicePath) {
    state.fiwareHeaderRequestsTotal += 1;
    state.servicesObserved.add(fiwareService);
    state.servicePathsObserved.add(fiwareServicePath);

    incrementCounter(state.fiwareRequestsByLabel, {
      fiware_service: fiwareService,
      fiware_service_path: fiwareServicePath,
      method,
      route,
      status_class: statusClass,
    });
  }
}

export function buildHealthPayload() {
  const memory = process.memoryUsage();

  return {
    status: 'UP',
    timestamp: new Date().toISOString(),
    uptimeSeconds: Math.floor(process.uptime()),
    process: {
      pid: process.pid,
      nodeVersion: process.version,
      memory: {
        rssBytes: memory.rss,
        heapTotalBytes: memory.heapTotal,
        heapUsedBytes: memory.heapUsed,
      },
    },
    roles: {
      apiServer: Boolean(config.roles.apiServer),
      fetcher: Boolean(config.roles.fetcher),
      syncQueries: Boolean(config.roles.syncQueries),
    },
    traffic: {
      totalRequests: state.totalRequests,
      errorRequests: state.totalErrorRequests,
      inFlightRequests: state.inFlightRequests,
      routesObserved: state.httpRequestsByLabel.size,
    },
    fiware: {
      requestsWithHeaders: state.fiwareHeaderRequestsTotal,
      servicesObserved: state.servicesObserved.size,
      servicePathsObserved: state.servicePathsObserved.size,
    },
  };
}

export function buildMetricsText() {
  const lines = [];
  const memory = process.memoryUsage();

  lines.push('# HELP fda_up Service liveness indicator (1=up).');
  lines.push('# TYPE fda_up gauge');
  lines.push('fda_up 1');

  lines.push('# HELP fda_info Service build/runtime information.');
  lines.push('# TYPE fda_info gauge');
  lines.push(
    `fda_info${formatLabels({
      version: SERVICE_VERSION,
      node_version: process.version,
      env: config.env,
      role_api_server: String(Boolean(config.roles.apiServer)),
      role_fetcher: String(Boolean(config.roles.fetcher)),
      role_sync_queries: String(Boolean(config.roles.syncQueries)),
    })} 1`,
  );

  lines.push(
    '# HELP fda_process_start_time_seconds Process start time since unix epoch in seconds.',
  );
  lines.push('# TYPE fda_process_start_time_seconds gauge');
  lines.push(`fda_process_start_time_seconds ${PROCESS_START_TIME_SECONDS}`);

  lines.push('# HELP fda_uptime_seconds Process uptime in seconds.');
  lines.push('# TYPE fda_uptime_seconds gauge');
  lines.push(`fda_uptime_seconds ${Math.floor(process.uptime())}`);

  lines.push(
    '# HELP fda_http_in_flight_requests Current in-flight HTTP requests.',
  );
  lines.push('# TYPE fda_http_in_flight_requests gauge');
  lines.push(`fda_http_in_flight_requests ${state.inFlightRequests}`);

  lines.push('# HELP fda_http_requests_total Total HTTP requests served.');
  lines.push('# TYPE fda_http_requests_total counter');
  lines.push(
    ...renderMetricLines('fda_http_requests_total', state.httpRequestsByLabel),
  );

  lines.push(
    '# HELP fda_http_request_duration_ms_sum Total HTTP request latency in milliseconds.',
  );
  lines.push('# TYPE fda_http_request_duration_ms_sum counter');
  lines.push(
    ...Array.from(state.httpDurationByLabel.values())
      .sort((a, b) => labelsKey(a.labels).localeCompare(labelsKey(b.labels)))
      .map(
        (entry) =>
          `fda_http_request_duration_ms_sum${formatLabels(entry.labels)} ${entry.sumMs}`,
      ),
  );

  lines.push(
    '# HELP fda_http_request_duration_ms_count Total number of timed HTTP requests.',
  );
  lines.push('# TYPE fda_http_request_duration_ms_count counter');
  lines.push(
    ...Array.from(state.httpDurationByLabel.values())
      .sort((a, b) => labelsKey(a.labels).localeCompare(labelsKey(b.labels)))
      .map(
        (entry) =>
          `fda_http_request_duration_ms_count${formatLabels(entry.labels)} ${entry.count}`,
      ),
  );

  lines.push(
    '# HELP fda_http_request_errors_total Total HTTP requests resulting in error status codes.',
  );
  lines.push('# TYPE fda_http_request_errors_total counter');
  lines.push(
    ...renderMetricLines(
      'fda_http_request_errors_total',
      state.httpErrorsByLabel,
    ),
  );

  lines.push(
    '# HELP fda_fiware_requests_total Total HTTP requests carrying FIWARE tenant headers.',
  );
  lines.push('# TYPE fda_fiware_requests_total counter');
  lines.push(
    ...renderMetricLines(
      'fda_fiware_requests_total',
      state.fiwareRequestsByLabel,
    ),
  );

  lines.push(
    '# HELP fda_fiware_catalog_services Distinct Fiware-Service values seen.',
  );
  lines.push('# TYPE fda_fiware_catalog_services gauge');
  lines.push(`fda_fiware_catalog_services ${state.servicesObserved.size}`);

  lines.push(
    '# HELP fda_fiware_catalog_service_paths Distinct Fiware-ServicePath values seen.',
  );
  lines.push('# TYPE fda_fiware_catalog_service_paths gauge');
  lines.push(
    `fda_fiware_catalog_service_paths ${state.servicePathsObserved.size}`,
  );

  lines.push(
    '# HELP fda_process_resident_memory_bytes Resident memory size in bytes.',
  );
  lines.push('# TYPE fda_process_resident_memory_bytes gauge');
  lines.push(`fda_process_resident_memory_bytes ${memory.rss}`);

  lines.push(
    '# HELP fda_process_heap_total_bytes Total V8 heap size in bytes.',
  );
  lines.push('# TYPE fda_process_heap_total_bytes gauge');
  lines.push(`fda_process_heap_total_bytes ${memory.heapTotal}`);

  lines.push('# HELP fda_process_heap_used_bytes Used V8 heap size in bytes.');
  lines.push('# TYPE fda_process_heap_used_bytes gauge');
  lines.push(`fda_process_heap_used_bytes ${memory.heapUsed}`);

  lines.push('# EOF');
  return `${lines.join('\n')}\n`;
}

export { getMetricsContentType };
