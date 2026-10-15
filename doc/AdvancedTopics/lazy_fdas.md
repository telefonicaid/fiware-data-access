# Lazy FDAs and the onDemand refresh policy

## Background

Creating a cached FDA fetches its data right away, and an `interval` or `window` FDA is refreshed on every
`refreshInterval` whether anybody queries it or not. An FDA is a cache of data that already lives in the source, so an
FDA that nobody reads wastes object storage and load on the source database.

Two options avoid that cost:

-   `lazy: true` delays the first fetch until the first query. From then on the FDA follows its refresh policy.
-   The `onDemand` refresh policy turns the FDA into a cache with an expiration time. The FDA is fetched on its first
    query and, after `maxAge`, on the next query that arrives. Nothing is refreshed while nobody queries it. An
    `onDemand` FDA is always lazy.

|                              | `lazy` + `none`     | `lazy` + `interval`                | `onDemand`                            |
| ---------------------------- | ------------------- | ---------------------------------- | ------------------------------------- |
| First fetch                  | First query         | First query                        | First query                           |
| Later refreshes              | Only manual (`PUT`) | Every `refreshInterval`, from then | First query after `maxAge`, or manual |
| Scheduled jobs               | None                | Created on the first query         | None                                  |
| Maximum age of the data read | Unbounded           | About `refreshInterval`            | `maxAge`                              |

`lazy` is not supported yet with the `window` refresh policy.

## Creation

```json
{
    "id": "alarms",
    "query": "SELECT * FROM public.alarms",
    "refreshPolicy": { "type": "onDemand", "params": { "maxAge": "15 minutes" } }
}
```

`maxAge` uses the same human interval format as `refreshInterval` (`30 seconds`, `15 minutes`, `1 hour`, `2 days`...).
`onDemand` accepts no other parameter. `lazy: false` together with `onDemand`, and `lazy` or `onDemand` on an only-fresh
FDA (`cached: false`), are rejected with `400 InvalidParam`.

The query is validated as for any other FDA, and the response is `202` with `"status": "idle"`. The FDA stays `idle` and
no job is scheduled. In `strict` mode the FDA still gets its empty, typed Parquet file and its `defaultDataAccess`,
because DAs are validated against that file. Writing it does not read any data from PostgreSQL (MongoDB reads one
document). The empty file is never served: queries are answered only after a fetch has completed.

## What a query does

The DA data endpoints and the CDA legacy endpoint behave the same way:

1.  If the FDA has data, and for `onDemand` the last fetch is more recent than `maxAge`, the query is answered right
    away.
2.  Otherwise the query asks for a fetch and waits. The request is an atomic update of the FDA in MongoDB, so with any
    number of concurrent queries and API instances only one fetch is enqueued, and the rest of the queries wait for it.
    For `lazy` + `interval` FDAs this is also when the recurring refresh jobs are created.
3.  The query polls the FDA every `FDA_ON_DEMAND_POLL_INTERVAL_MS` until one of these happens:
    -   The fetch completes: the query is answered with the new data.
    -   The fetch fails: the query returns `503 FDAMaterializationFailed` with the error of the FDA (for example
        `StorageQuotaExceeded`).
    -   `FDA_ON_DEMAND_WAIT_TIMEOUT_MS` passes: the query returns `409 FDAUnavailable`. The fetch goes on, and a later
        query gets the data once it completes.

Expiration is strict: an expired `onDemand` FDA never answers with its old data, so the first query after `maxAge` pays
for a full fetch. Waiting does not hold a DuckDB connection.

The fetch itself runs in an instance with the fetcher role, like any other refresh. If no fetcher is running, queries
wait until the timeout.

## Failures

A failed fetch leaves the FDA `failed`. For `FDA_ON_DEMAND_RETRY_BACKOFF_MS` after the failure, queries return
`503 FDAMaterializationFailed` at once instead of retrying, so a broken source is not queried again on every request.
After the backoff, the next query tries again. A manual refresh (`PUT /{visibility}/fdas/{fdaId}`) ignores the backoff.

## Manual refresh and deletion

-   `PUT /{visibility}/fdas/{fdaId}` on an FDA that has never been fetched enqueues its first fetch without waiting for
    it, and also creates the recurring jobs of a `lazy` + `interval` FDA. If a fetch is already requested or running it
    returns `409 AlreadyFetching`. On an FDA that already has data it works as for any other FDA.
-   An `idle` FDA can be deleted.

## Configuration

| Variable                         | Default | Description                                                                          |
| -------------------------------- | ------- | ------------------------------------------------------------------------------------ |
| `FDA_ON_DEMAND_WAIT_TIMEOUT_MS`  | `30000` | How long a query waits for an on-demand fetch before returning `409 FDAUnavailable`. |
| `FDA_ON_DEMAND_POLL_INTERVAL_MS` | `500`   | How often a waiting query checks the FDA in MongoDB.                                 |
| `FDA_ON_DEMAND_RETRY_BACKOFF_MS` | `60000` | Time after a failed on-demand fetch during which queries fail without retrying.      |
