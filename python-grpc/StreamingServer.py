from concurrent import futures
from Reader import ParquetReaderFactory
import grpc
import stream_pb2
import stream_pb2_grpc
import json
import os

def split_array(arr, size):
    """Split an array into smaller arrays of the specified size."""
    for i in range(0, len(arr), size):
        yield arr[i:i + size]

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

class ParquetStreamServicer(stream_pb2_grpc.ParquetStreamServicer):
    def __init__(self, grpc_batch_size):
        self.grpc_batch_size = grpc_batch_size

    def ProcessFile(self, request, context):
        print(f"Received request for files: {request.file_path}")
        # for file_path in request.file_paths:
        reader = ParquetReaderFactory.create_reader(request.file_path)
        if reader.is_valid():
            total_row_groups = reader.parquet_file.num_row_groups
            for i in range(total_row_groups):
                print(f"Processing file: {request.file_path} row group: {i}")
                df = reader.read_row_group(i)
                print(f"Read row group: {i} with {len(df)} rows")
                batches = list(split_array(df, self.grpc_batch_size))

                print(f"Splitting into {len(batches)} batches of {self.grpc_batch_size} rows each")
                for batch_index, batch in enumerate(batches):
                    yield stream_pb2.FileResponse(
                        file_path=request.file_path,
                        total_row_groups=total_row_groups,
                        row_group_index=i,
                        row_count=len(df),
                        batch_index=batch_index,
                        batch_size=len(batch),
                        batch_count=len(batches),
                        data=json.dumps(batch, cls=DatetimeEncoder)
                    )

def start_grpc_server(grpc_port, grpc_batch_size):
    print("Starting server...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ParquetStreamServicer(grpc_batch_size)
    stream_pb2_grpc.add_ParquetStreamServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{grpc_port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    grpc_port = os.environ["GRPC_PORT"]
    grpc_batch_size = int(os.environ["GRPC_BATCH_SIZE"])
    start_grpc_server(grpc_port, grpc_batch_size)
