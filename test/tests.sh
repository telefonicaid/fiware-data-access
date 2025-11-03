#!/bin/bash
# Script to store a set and query it with apache bench

# Send POST request to storeSet
curl -i -X POST http://localhost:8081/storeSet \
  -d '{"cda": "dumps", "filePath": "test/examples/dumps.csv", "path": "s3://my-bucket-api/output/"}' \
  -H 'content-type: application/json'

# Run ApacheBench (ab) performance test
ab -n 1000 -c 2 -p data.json -T "application/json" http://localhost:8080/queryFDA
