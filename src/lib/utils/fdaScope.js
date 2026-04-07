export function normalizeScopedServicePath(servicePath) {
  if (!servicePath || typeof servicePath !== 'string') {
    throw new Error('servicePath is required');
  }

  const normalizedServicePath = servicePath.trim();
  if (!normalizedServicePath) {
    throw new Error('servicePath is required');
  }

  return normalizedServicePath;
}

export function getFDAStoragePath(fdaId, servicePath) {
  const normalizedServicePath = normalizeScopedServicePath(servicePath);

  const servicePathScope =
    normalizedServicePath === '/'
      ? '_root'
      : normalizedServicePath.replace(/^\//, '');

  return `${servicePathScope}/${fdaId}`;
}

export function buildFDAJobFilter(name, service, fdaId, servicePath) {
  return {
    name,
    'data.service': service,
    'data.fdaId': fdaId,
    'data.servicePath': servicePath,
  };
}
