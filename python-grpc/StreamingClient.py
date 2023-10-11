import grpc
import stream_pb2
import stream_pb2_grpc
import pandas as pd
import json, os

# Initialize a channel and stub
channel = grpc.insecure_channel(f'{os.environ["GRPC_HOST"]}:{os.environ["GRPC_PORT"]}')
stub = stream_pb2_grpc.ParquetStreamStub(channel)

# Specify file paths and make the request
with open(os.path.join(os.environ["ROOT_FOLDER"], "file_paths.txt")) as f:
    file_paths = f.read().splitlines()

response_iterator = stub.ProcessFiles(stream_pb2.FileRequest(file_paths=file_paths))

tracker = pd.DataFrame()
# Process the streamed responses
for response in response_iterator:
    print(f"Received data for {response.file_path}, row group {response.row_group_index}:")
    x = pd.DataFrame([{
        'file_path': response.file_path,
        'row_group_index': response.row_group_index,
        'total_row_groups': response.total_row_groups,
        'data': response.data
        }])
    tracker = pd.concat([
        tracker,
        pd.DataFrame([{
          'file_path': response.file_path,
          'row_group_index': response.row_group_index,
          'total_row_groups': response.total_row_groups,
          'data': response.data
        }])
      ], ignore_index=True)

    df = tracker.groupby(['file_path', 'total_row_groups']).agg({'row_group_index': 'nunique'}).reset_index()
    df = df[df['row_group_index'] == df['total_row_groups']]

    for file_path in df['file_path']:
        print(f"Received all data for {file_path}")
        csv_file_path = os.path.join(os.environ["OUTPUT_FOLDER"], f"{file_path}.csv")
        csv_df = pd.DataFrame()

        items = tracker[tracker['file_path'] == file_path].sort_values(by=['row_group_index'])

        for index, row in items.iterrows():
            parsed_df = pd.DataFrame(json.loads(row['data']))
            csv_df = pd.concat([csv_df, parsed_df], ignore_index=True)

        csv_df.to_csv(csv_file_path, index=False)
        print(f"Saved data for {file_path} to {csv_file_path}")
        tracker = tracker[tracker['file_path'] != file_path]

