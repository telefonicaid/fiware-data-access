# CDA Legacy Compatibility

## Scope

This note documents how FDA keeps backward compatibility with legacy Pentaho CDA clients.

Compatibility endpoint:

-   `GET /plugin/cda/api/doQuery`
-   `POST /plugin/cda/api/doQuery`

## Why this exists

Legacy CDA clients (for example old BI and CKAN integrations) usually send requests with:

-   no FIWARE headers
-   transport-level query parameters (`param*`, `pageSize`, `pageStart`)
-   `path` as the source of tenant/scope context

FDA keeps this behavior intentionally in the compatibility endpoint to avoid forcing migrations in those clients.

## Scope resolution convention

In CDA compatibility mode, context is derived from `path`:

-   `service`: extracted from `path`
-   `visibility`: extracted from `path` (`public` or `private`)
-   `servicePath`: normalized as `/${visibility}`

This mirrors historic usage where `Fiware-ServicePath` was not part of the CDA contract.

Supported path styles include:

-   `/public/<service>/verticals/sql/<fdaId>`
-   `/private/<service>/verticals/sql/<fdaId>`
-   `home/<service>/verticals/public/<fdaId>.cda`
-   `home/<service>/verticals/private/<fdaId>.cda`

If `fdaId` cannot be inferred from path, it falls back to `dataAccessId`.

## Request mapping

Input fields are mapped as follows:

-   `dataAccessId` -> DA id
-   `param*` -> DA params (prefix removed)
-   `pageSize`, `pageStart` -> forwarded as-is
-   `outputType` -> response format (`json`, `csv`, `xls`)

Unsupported compatibility feature:

-   `param_not_*`

## Behavior notes

-   For `json`, response is CDA-like: `{ metadata, resultset, queryInfo }`.
-   For `csv` and `xls`, FDA returns file downloads.
-   `_TRUST_USER_` can be present in requests and is ignored by FDA.

## Recommended usage

-   Use this endpoint only for legacy clients that require CDA wire format.
-   Use native FDA endpoints for new integrations.
