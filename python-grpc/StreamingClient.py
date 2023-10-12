import grpc
import stream_pb2
import stream_pb2_grpc
import pandas as pd
import json
import os

class ParquetDataProcessor:
    def __init__(self, grpc_host, grpc_port, root_folder, output_folder):
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self.root_folder = root_folder
        self.output_folder = output_folder
        self.tracker = pd.DataFrame()
        self.init_grpc()

    def init_grpc(self):
        channel = grpc.insecure_channel(f'{self.grpc_host}:{self.grpc_port}')
        self.stub = stream_pb2_grpc.ParquetStreamStub(channel)

    def process_files(self):
        file_paths = self.load_file_paths()
        for file_path in file_paths:
            response_iterator = self.stub.ProcessFile(stream_pb2.FileRequest(file_path=file_path))
            for response in response_iterator:
                print(f"Received data for {os.path.basename(response.file_path)}, row group {response.row_group_index}, batch {response.batch_index}:")
                self.process_response(response)

    def find_completed_files(self):
        if self.tracker.empty:
            return []

        df = self.tracker.groupby(['file_path', 'total_row_groups', 'row_group_index', 'row_count']).agg({'batch_size': 'sum'}).reset_index()
        df = df[df['row_count'] == df['batch_size']].groupby(['file_path', 'total_row_groups']).agg({'row_group_index': 'nunique'}).reset_index()

        completed = df[df['row_group_index'] == df['total_row_groups']]
        return completed['file_path']

    def combine_batches(self, file_path):
        batches = self.tracker[self.tracker['file_path'] == file_path].sort_values(by=['row_group_index', 'batch_index']).apply(lambda x: f'{os.path.basename(x.file_path)}_{x.row_group_index}_{x.batch_index}.json', axis=1)
        df = pd.DataFrame()

        for batch_file_name in batches:
            batch_file_path = os.path.join(self.output_folder, batch_file_name)
            with open(batch_file_path, "r") as f:
                data = json.load(f)
                parsed_df = pd.DataFrame(data)
                df = pd.concat([df, parsed_df], ignore_index=True)
            os.remove(batch_file_path)

        df.to_csv(os.path.join(self.output_folder, f"{os.path.basename(file_path)}.csv"), index=False)
        print(f"Completed processing file: {os.path.basename(file_path)}")

    def process_response(self, response):
        response_df = pd.DataFrame([{
            'file_path': response.file_path,
            'row_group_index': response.row_group_index,
            'total_row_groups': response.total_row_groups,
            'row_count': response.row_count,
            'batch_index': response.batch_index,
            'batch_size': response.batch_size,
            'batch_count': response.batch_count
        }])

        if (response.total_row_groups == 1 and response.batch_count == 1):
            df = pd.DataFrame(json.loads(response.data))
            csv_file_path = os.path.join(self.output_folder, f"{os.path.basename(response.file_path)}.csv")
            df.to_csv(csv_file_path, index=False)
        else:
            temp_json_file = os.path.join(self.output_folder,f'{os.path.basename(response.file_path)}_{response.row_group_index}_{response.batch_index}.json')
            print(temp_json_file)
            with open(temp_json_file, "w") as f:
                f.write(response.data)
            self.tracker = pd.concat([self.tracker, response_df], ignore_index=True)

        file_path = response.file_path
        completed = self.find_completed_files()

        for file_path in completed:
            self.combine_batches(file_path)
            self.tracker = self.tracker[self.tracker['file_path'] != file_path]

    def load_file_paths(self):
        with open(os.path.join(self.root_folder, "file_paths.txt")) as f:
            file_paths = f.read().splitlines()
        return file_paths

if __name__ == "__main__":
    processor = ParquetDataProcessor(
        grpc_host=os.environ["GRPC_HOST"],
        grpc_port=os.environ["GRPC_PORT"],
        root_folder=os.environ["ROOT_FOLDER"],
        output_folder=os.environ["OUTPUT_FOLDER"]
    )
    processor.process_files()
