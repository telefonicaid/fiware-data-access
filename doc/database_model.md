-   [Introduction](#introduction)
-   [FDAs collection](#fdas-collection)

## Introduction

This document reflects the structure the `Fiware-Data-Access` has on `MongoDB` to manage the different `FDAs` and `DAs`.
This schema acts as a central point of information for the application, it reflects the changes commited on the bucket
storage application and stores information about the database that contains the source information.

`Fiware-Data-Access` uses one collection in the database, described in the following subsection.

## FDAs collection

The _fdas_ collection stores information about `fiware-data-access` objects, the multiple `data-access` that each `fda`
has and the necessary information to manage them. Each document in the collection corresponds to one `fda`.

Fields:

-   **\_id**: stores the unique id of the document, automatically created by MongoDB.
-   **fdaId**: the id of the FDA. The app creates an index using _fdaId_ and _service_ so the combination of the two is
    unique.
-   **service**: _fiware-service_ of the fda. The app creates and index using _fdaId_ and _service_ so the combination
    of the two is unique.
-   **description**: description of the FDA.
-   **database**: name of the _postgre_ database from which the _fda_ fetches the data for its _das_.
-   **schema**: name of the _postgre_ schema from which the _fda_ fetches the data for its _das_.
-   **table**: name of the _postgre_ table from which the _fda_ fetches the data for its _das_.
-   **path**: path to the data in the bucket storage application that is going to be queried by the _da_.
-   **das**: keymap of the different _DAs_ in the _FDA_. Each _Da_ (key included) is created by the user and has the
    following information:
    -   **description**: basic description of the _DA_.
    -   **query**: parameterized sql query that retrieves data from the file in the bucket storage application defined
        in the _FDA_. The query must be a string (inside single quotes ' ') and the parameters are the name of the value
        preceded by a _$_.

Example document:

```
{
    _id: ObjectId('695f9a3cc0d41d928f5e6a39'),
    fdaId: 'fda1',
    description: 'Description for the first FDA',
    database: 'exampleDatabase',
    schema: 'exampleSchema',
    table: 'exampleTable',
    path: '/das/exampleTable.parquet',
    das: {
        da1: {
            description: 'First DA querying timeInstant and population.',
            query: 'SELECT * FROM \'s3://my-bucket/das/exampleTable.parquet\' WHERE population = $population
            AND timeinstant = $timeinstant;'
        }
        da2: {
            description: 'Second DA querying timeInstant and gender.',
            query: 'SELECT * FROM \'s3://my-bucket/das/exampleTable.parquet\' WHERE gender = $gender
            AND timeinstant = $timeinstant;'
        }
    },
    service: 'fiwareService'
}
```
