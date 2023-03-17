# transitlog-apc-archive-sink [![Test and create Docker image](https://github.com/HSLdevcom/transitlog-apc-archive-sink/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitlog-apc-archive-sink/actions/workflows/test-and-build.yml)

Sink for writing APC (i.e. passenger count) data to Parquet files, which are stored in Blob Storage. Parquet schema for APC data can be found [here](./src/main/resources/apc_parquet_schema.txt)

## Building

Building runnable JAR:
```bash
./gradlew shadowJar
```