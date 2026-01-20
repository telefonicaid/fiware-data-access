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

### Bucket name convention

When creating a _FDA_ we need to indicate the origin of the data we want to query and where we want to store it inside a
_bucket_ in our storage system, but the name of the _bucket_ is fixed in the code. The name of the _bucket_ the _FDA_ is
gonna search for when uploading data is the **same** as the `fiware-service` of the _FDA_. \
The `FDA` app **does not** create the bucket when uploading a _FDA_ to the object bucket-based storage system, it instead
rises an error. This is because we want to manage the bucket permissions so we prefer to create the bucket previously by
hand.
