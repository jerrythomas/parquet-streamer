# Streaming Parquet to CSV

This repo implements reading parquet files one row group at a time and streams it over the network using GRPC. The client connects to the GRPS server and requests multiple files. Each file is processed and the client captures streaming data into separate CSV files.

> This code relies on `pyarrow` library, specifically the function read_row_group which takes care of the heavy lifting in terms of reading one row group from the parquet file.

## Setup

```bash
cd python
poetry shell
poetry install
```

## Setup the environment

Update the `.env` file. Provide AWS access keys if you need to process S3 files.

```bash
AWS_ACCESS_KEY=
AWS_SECRET_KEY=
AWS_REGION=
ROOT_FOLDER=../input
OUTPUT_FOLDER=../output
GRPC_PORT=50051
GRPC_HOST=localhost
GRPC_BATCH_SIZE=1000
```

Create a file with `$ROOT_FOLDER\file_paths.txt` containing list of parquet files to process.

```bash
export ENV='../.env'
set -o allexport && source ${ENV} set +o allexport
poetry shell
```

## Start the server

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. stream.proto
python StreamingServer.py
```

## Start the client

In a separate tab, set up the environment and

```bash
python StreamingClient.py
```

## Parquet Analysis

The Parquet file itself is complex and it is a non trivial task to implement all possible features for reading each row_group without using an existing library.

- The file starts and ends with a 4 byte magic number 'PAR1'
- File metadata size can be obtained from 4 bytes prior to the magic number at the end.
- Using the metadata size and offset we can read the file metadata
- This provides us information on the row groups and columns within row groups

Use the following command to print row group information for a file.

```bash
python lib/LowLevel.py ../../resources/CreateRole.test.gz.parquet
```
