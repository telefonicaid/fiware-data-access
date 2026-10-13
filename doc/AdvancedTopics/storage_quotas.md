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

Older `mc` releases use `mc admin bucket quota local/<bucket> --hard 10GiB` instead of `mc quota`.

The quota counts every object of the bucket, so it includes the temporary objects FDA creates during a fetch (see below)
and the objects of every FDA of the service.

### How MinIO enforces the quota

The quota is **approximate**. MinIO rejects a write when the usage computed by its periodic data scanner, plus the size
of the request, exceeds the quota. Writes made since the last scan are not counted, and neither are deletions. Measured
on a bucket with a 10 MiB quota:

-   Objects of 3 MB each were accepted until the bucket held 13 MB; MinIO only started rejecting writes about 30 to 45
    seconds later, when the scanner updated the usage.
-   A single request bigger than the quota is rejected immediately.
-   After deleting objects from a full bucket the writes kept being rejected for more than two minutes, while `mc du`
    already showed the real, lower size. Raising the quota takes effect immediately.

So the quota limits the growth of a service, but it is not a ceiling to the byte: the bucket can go over it for a short
time, and space freed by a failed or cleaned fetch is not available again until MinIO scans the bucket. If a refresh
fails with `StorageQuotaExceeded` right after freeing space, wait a few minutes or raise the quota.

### Sizing

During a fetch of an FDA, FDA needs room in the bucket for objects that live only while the fetch runs:

1.  The staging CSV (`<servicePath>/<fdaId>.csv`).
2.  The new Parquet written under `tmp/<servicePath>/<fdaId>.parquet`, while the previous version is still published.
3.  The publication: one `copy` per file (the new Parquet, or each rewritten partition) over its final key, followed by
    the delete of the `tmp/` object. The CSV has already been deleted at that point.

Leave room in the quota for the CSV plus the new Parquet of the largest FDA of the service, and for the new Parquet once
more at the publish step, because MinIO does not count deletions until it scans the bucket. With several refresh jobs
running at once (`FDA_MAX_CONCURRENT_REFRESH_JOBS`) the needs add up.

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

### Limitation: partitioned FDAs and the publish step

Non partitioned FDAs publish their new Parquet with a single copy, which either replaces the previous one or leaves it
as it was. Partitioned FDAs publish partition by partition, and object storage has no operation to replace several
objects at once. If the quota is hit in the middle of the publication, the FDA ends `failed` and the `tmp/` objects and
the CSV are removed as in any other case, but the partitions already copied hold the new data while the rest keep the
previous one. Each partition is complete and valid on its own; what can differ is the fetch they come from.

The state is not repaired by FDA at that moment. The refresh flow converges on its own: the next successful refresh
rewrites the partitions of its window (or all of them if the FDA has no window), and when the failed fetch was a rebuild
because the source schema changed, the stored schema is only updated after a successful publication, so the next refresh
detects the change again and rebuilds. The failure handling of this step is covered by a unit test, and an integration
test builds the mixed state by hand and checks that the next refresh repairs it.

Because MinIO does not count deletions until it scans the bucket (see above), leaving the headroom for the publish step
described in the sizing section is what avoids this case.

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

One limit of the MinIO option to keep in mind: its quota is approximate (see above). If an exact limit per service is
needed, only a measurement done by FDA can provide it.

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
