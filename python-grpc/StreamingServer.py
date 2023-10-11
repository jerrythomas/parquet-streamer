from concurrent import futures
from Reader import ParquetReaderFactory
import grpc
import stream_pb2
import stream_pb2_grpc
import json
import os

class ParquetStreamServicer(stream_pb2_grpc.ParquetStreamServicer):

    def ProcessFiles(self, request, context):
        for file_path in request.file_paths:
            reader = ParquetReaderFactory.create_reader(file_path)
            if reader.is_valid():
                total_row_groups = reader.parquet_file.num_row_groups
                for i in range(total_row_groups):
                    df = reader.read_row_group(i)
                    yield stream_pb2.FileResponse(
                        file_path=file_path,
                        total_row_groups=total_row_groups,
                        row_group_index=i,
                        data=json.dumps(df)
                    )

# Start the server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
stream_pb2_grpc.add_ParquetStreamServicer_to_server(ParquetStreamServicer(), server)
server.add_insecure_port(f'[::]:{os.environ["GRPC_PORT"]}')
server.start()
server.wait_for_termination()
