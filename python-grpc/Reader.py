from abc import ABC, abstractmethod
import os
import pyarrow.parquet as pq
import pyarrow.fs
import pandas as pd

# Common interface
class ParquetReader(ABC):

    @abstractmethod
    def is_valid(self):
        pass

    @property
    def parquet_file(self):
        if self._parquet_file is None:
            self._parquet_file = pq.ParquetFile(self.path, filesystem=self.fs)
        return self._parquet_file

    def read_row_group(self, row_group_index):
        if (row_group_index < self.parquet_file.num_row_groups):
            table = self.parquet_file.read_row_group(row_group_index)
            return table.to_pylist()
        else:
            return None


# Concrete implementation for local files
class LocalParquetReader(ParquetReader):

    def __init__(self, path):
        self.path = os.path.join(os.environ['ROOT_FOLDER'],path)
        self.fs = pyarrow.fs.LocalFileSystem()
        self._parquet_file = None

    def is_valid(self):
        with open(self.path, 'rb') as f:
            f.seek(-4, 2)  # Move to 4 bytes from the file end
            return f.read() == b'PAR1'

# Concrete implementation for S3
class S3ParquetReader(ParquetReader):

    def __init__(self, path):
        self.path = path[5:]  # remove s3:// prefix
        self.fs = pyarrow.fs.S3FileSystem(
            access_key=os.environ['AWS_ACCESS_KEY'],
            secret_key=os.environ['AWS_SECRET_KEY'],
            region=os.environ['AWS_REGION']
        )
        self._parquet_file = None


# Factory to create appropriate reader
class ParquetReaderFactory:
    @staticmethod
    def create_reader(path):
        if path.startswith("s3://"):
            return S3ParquetReader(path)
        else:
            return LocalParquetReader(path)
