# Storage quotas

## Background

A tenant can create any number of FDAs of any size, and nothing in FDA stops the object storage from filling up. Each
`Fiware-Service` already has its own bucket (see
[Bucket name convention](/doc/05_advanced_topics.md#bucket-name-convention)), so the quota of a service is the quota of
its bucket. FDA relies on **MinIO hard bucket quotas**, configured by the operator, and handles the failure when the
quota is reached.

## Operator guide

Quotas are set with the MinIO client (`mc`) against the bucket of the service. In `docker/docker-compose.yml` the `mc`
container already has the alias `local`.

| Action                | Command                                    |
| --------------------- | ------------------------------------------ |
| Set a hard quota      | `mc quota set local/<bucket> --size 10GiB` |
| Review the quota      | `mc quota info local/<bucket>`             |
| Review the real usage | `mc du local/<bucket>`                     |
| Remove the quota      | `mc quota clear local/<bucket>`            |

With the provided `docker compose`, setting `FDA_BUCKET_QUOTA` (for example `FDA_BUCKET_QUOTA=10GiB`) makes the `mc`
container apply that quota to the `trantor` bucket when it starts. It is not set by default.

Older `mc` releases use `mc admin bucket quota local/<bucket> --hard 10GiB` instead of `mc quota`.

The quota counts every object of the bucket, so it includes the temporary objects FDA creates during a fetch (see below)
and the objects of every FDA of the service.

### Sizing

During a fetch of an FDA, FDA needs room in the bucket for objects that live only while the fetch runs:

1.  The staging CSV (`<servicePath>/<fdaId>.csv`).
2.  The new Parquet written under `tmp/<servicePath>/<fdaId>.parquet`, while the previous version is still published.
3.  A second copy of the new Parquet when it is published (copy to the final key, then delete of the `tmp/` object). The
    CSV has already been deleted at that point. For partitioned FDAs only the rewritten partitions are copied.

Leave free quota for at least the CSV plus the new Parquet of the largest FDA of the service, and for twice the new
Parquet size at the publish step. With several refresh jobs running at once (`FDA_MAX_CONCURRENT_REFRESH_JOBS`) the
needs add up.

### Quota accounting in MinIO

MinIO evaluates the quota against the usage it accounts for the bucket, not against a fresh listing. In the tests done
for this feature, after deleting objects from a full bucket the writes were still rejected for more than two minutes
while `mc du` already showed the lower size. Raising the quota takes effect immediately. Take this into account when
reading a quota failure right after freeing space: either wait for MinIO to refresh its usage or raise the quota.

## Behavior when the quota is reached

When a write is rejected by MinIO with `Bucket quota exceeded` (error code `XMinioAdminBucketQuotaExceeded`), FDA
reports it as:

| HTTP status | Error type             | Description                                                                                                     |
| ----------- | ---------------------- | --------------------------------------------------------------------------------------------------------------- |
| 507         | `StorageQuotaExceeded` | `Storage quota exceeded for bucket "<bucket>". Free up space or ask the operator to increase the bucket quota.` |

The error can happen at three points of a fetch: while the CSV is staged, while DuckDB writes the Parquet, and while the
Parquet is published. In all of them:

-   The FDA ends in `failed` and its `error` field contains the message above, instead of a generic upload or DuckDB
    error. Fetches run in the background, so the 507 status is only returned by the requests that fail synchronously
    (for example creating an FDA or an upload FDA); for the rest, check `error` in `GET /{visibility}/fdas/{fdaId}`.
-   The staging CSV and the objects under `tmp/` are deleted. Deleting objects is not blocked by the quota.
-   The previously published Parquet is not touched: the new data is always written under `tmp/` first and only replaces
    the published object once it is complete, so the FDA keeps answering queries with its last good data. A failed
    multipart upload is aborted by the S3 client and leaves no object.

Once there is room again, the next scheduled or manual refresh (`PUT /{visibility}/fdas/{fdaId}`) completes normally.

## Decision: FDA-managed quotas

The branch `poc/resource-governance` implements quotas managed by FDA (per service, per `servicePath`, per FDA, per
fetch and number of FDAs, plus a `GET /usage` endpoint). It was used to compare both options:

|                       | MinIO bucket quota   | FDA-managed quotas                                    |
| --------------------- | -------------------- | ----------------------------------------------------- |
| Granularity           | Service (bucket)     | Service, servicePath, single FDA, number of FDAs      |
| Visible to the tenant | No                   | Yes, limits and remaining room                        |
| When it acts          | During the write     | Before creating, during the fetch, or after measuring |
| Who manages it        | Operators, with `mc` | API or configuration                                  |
| Cost for us           | Error handling only  | New code, storage and API to maintain                 |

**Decision: the MinIO bucket quota with the error handling described above is enough for now.** The risk to cover is the
object storage filling up, and the quota of the service is enforced by the storage itself, with no measurement or
accounting code in FDA that could drift from the real usage.

FDA-managed quotas are left out until one of these needs appears:

-   Tenants need to see their limits and remaining room through the API.
-   A limit is needed below the service level (per `servicePath` or per FDA).
-   A limit on the number of FDAs is needed, which the storage cannot enforce.

If they are added, a quota rejection raised by FDA before or during the fetch should use a different error type from
`StorageQuotaExceeded` (which means the storage itself refused the write), so that clients and operators can tell a
tenant limit from a full bucket.
