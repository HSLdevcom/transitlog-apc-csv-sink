# transitlog-apc-archive-sink [![Test and create Docker image](https://github.com/HSLdevcom/transitlog-apc-archive-sink/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitlog-apc-archive-sink/actions/workflows/test-and-build.yml)

Sink for writing APC (i.e. passenger count) data to Parquet files, which are stored in Blob Storage. 

## Building

Building runnable JAR:
```bash
./gradlew shadowJar
```

## Running

Use Gradle or Docker to run the service locally. Connection to Apache Pulsar is needed. Two environment variables must be specified:
* `BLOB_CONNECTION_STRING` - connection string to the blob storage
* `BLOB_CONTAINER` - name of the blob container to be used

## Data format

Data is written to Parquet files for which the schema can be found [here](./src/main/resources/apc_parquet_schema.txt). 

Each file contains data for 15 minutes, based on the time the data was _received_. File names are in format `apc_<date>T<hour>-<minute>.parquet`, where `<date>` is date in ISO8601 format, `<hour>` is hour of the day (0-23) and `<minute>` is 1-4 for each quarter of the hour. File name uses UTC timezone.

Metadata and index tags are added to the blob when it is uploaded to Blob Storage. Metadata are `row_count`, which is the amount of rows in the Parquet file, and `parquet_crc`, which is the CRC code of the file contents encoded in Base64. Index tags are `min_tst`, which is the smallest timestamp (`tst`) in the file, and `max_tst`, which is the largest timestamp in the file.
