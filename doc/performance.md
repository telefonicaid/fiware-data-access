# Performance Measuring Utilities

The `performance.js` file provides a series of methods to measure the performance of certain processes. The performance
is measured as the execution time of the evaluated action (not counting the connection time).

## Methods

The performance file measures the individual steps of certain actions removing the dependency between them. Each method
takes the same parameters as the action they measure:

### storeSetPerformance

Measures the process of creating a set from PostgreSQL. The isolated steps are retrieving a table from postgresql using
streams, sending local data to Minio using AWS sdk and using DuckDb to change the Minio set format from CSV to PARQUET.
The methods used to measure the steps are the following:

* **pgToNode**: downloads a table from postgres using postgresql native functionality to export data in CSV format. The retrieved data is discarded in a sink so data processing doesn't
affect performance.
* **nodeToMinio25MBChunk1Parallel**: uploads data from a local csv file with the same name as the table using multipart upload. The size of each chunk is 25Mb.
* **nodeToMinio25MBChunk4Parallel**: uploads data from a local csv file with the same name as the table using multipart upload. The upload is configured to use 25Mb chunks and process 4 chunks in parallel.
* **nodeToMinio5MBChunk1Parallel**: uploads data from a local csv file with the same name as the table using multipart upload. The upload is configured to use 5Mb chunks.
* **changeFormat**: connects to Minio using DuckDb and changes the format of the previously uploaded table from CSV to PARQUET.

## Use

To use the method you simply must call it in the action it is measuring and pass the arguments the real action needs.

Usage example:

```
export async function storeSetPG(bucket, database, table, fda) {
  await storeSetPerformance(bucket, database, table, fda);
  .
  .
  .
}
```

## Example results

Example results from an execution of the method `storeSetPerformance` compared to a complete execution (this is part of a PoC to validate the approach of the proyect, run in November 2025):

| Table Size | Complete execution | pgToNode   | nodeToMinio | changeFormat |
| :--------- | :----------------- | :--------- | :---------- | :----------- |
| 195,5 MB   | 20,648s            | 16,673s    | 6,171s      | 2,707s       |
| 902,4 MB   | 4m 25,248s         | 1m 30,702s | 29,629s     | 5,482s       |
| 3,1 GB     | 6m 35,771s         | 5m 39,613s | 1m 45,102s  | 14,832s      |
| 6,6 GB     | 9m 41,120s         | 7m 26,220s | 3m 28,672s  | 44,618s      |
