# Advanced Topics

-   [Introduction](#introduction)
-   [Object Bucket-Based Storage System](#object-bucket-based-storage-system)
    -   [Bucket Name convention](#bucket-name-convention)

## Introduction

The `FDA` app interacts and communicates with various systems. To keep that communication tidy we have multiple
documentation files describing the different structures and functionalities involved. This file documents the advanced
topics and FDA internals that don't fit in the other documents.

## Object Bucket-Based Storage System

`FDA` uses an object bucket-based storage system to efficiently store the data to query. In this section we have the
topics related to this system.

-   [Bucket name convention](#bucket-name-convention)
-   [File name convention](#file-name-convention)
-   [Data origin](#data-origin)

### Bucket name convention

When creating a _FDA_ we fetch the data from `PostgreSQL` and store inside a _bucket_ in our storage system. The name of
this _bucket_ is fixed and is the **same** as the `fiware-service` of the _FDA_. \
The `FDA` app **does not** create the bucket when uploading a _FDA_ to the object bucket-based storage system, it instead
rises an error. This is because we want to manage the bucket permissions so we prefer to create the bucket previously by
hand.

### File name convention

The `PostgreSQL` data is always uploaded to the `Object Bucket-Based Storage System` in a _parquet_ file using the
_fdaId_ as the name. This file is updated and removed in unity with the _fda_ information in `MongoDb`.

### Data origin

When creating a _FDA_ we cannot specify the database from where we want to fetch the data. In _FDA_ we are always gonna
use the `fiware-service` value as the _database_ name.
