# ScratchDB

ScratchDB is a wrapper around Clickhouse that lets you input arbitrary JSON and
perform analytical queries against it. It automatically creates tables
and columns when new data is added.

## Quickstart

#### 1. Run the server

Clone the repo:
```bash
$ git clone git@github.com:scratchdata/ScratchDB.git
$ cd ScratchDB
```

Start clickhouse and localstack:
``` bash
$ docker-compose up
```

In a separate terminal, start the insert service:
``` bash
$ go run . insert
```

Finally, in an additional terminal window, start the ingest service:
```bash
$ go run . ingest
```

#### 2. Insert JSON data

``` bash
$ curl -X POST http://localhost:3000/data \
    -H 'Content-Type: application/json' \
    -H 'X-Api-Key: local' \
    -d '{"table":"my_table","data":{"fruit": "apple"}}'
```

#### 3. Query

To view data in JSON format: [http://localhost:3000/query?q=select * from my_table](http://localhost:3000/query?q=select%20*%20from%20my_table)

```bash
curl -H 'X-Api-Key: local' "http://localhost:3000/query?q=select%20*%20from%20my_table"
```

To view data in an HTML table: [http://localhost:3000/query?format=html&q=select * from my_table](http://localhost:3000/query?format=html&q=select%20*%20from%20my_table)
```
curl -H 'X-Api-Key: local' "http://localhost:3000/query?format=html&q=select%20*%20from%20my_table"
```
