export const USAGE_SUM_FIELDS = {
  bytes: 'storage.bytes',
  objects: 'storage.objects',
  partitions: 'storage.partitions',
  queries: 'access.count',
  sinceLastFetch: 'access.sinceLastFetch',
  refreshDurationMs: 'lastRefresh.durationMs',
  refreshBytesFetched: 'lastRefresh.bytesFetched',
};

export function summarizeUsage(groups) {
  const fields = ['fdas', ...Object.keys(USAGE_SUM_FIELDS)];
  const totals = Object.fromEntries(fields.map((field) => [field, 0]));
  totals.lastAccessAt = null;
  for (const group of groups) {
    for (const field of fields) {
      totals[field] += group[field] ?? 0;
    }
    if (
      group.lastAccessAt &&
      (!totals.lastAccessAt ||
        new Date(group.lastAccessAt) > new Date(totals.lastAccessAt))
    ) {
      totals.lastAccessAt = group.lastAccessAt;
    }
  }
  return totals;
}
