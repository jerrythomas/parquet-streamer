from fastparquet.cencoding import from_buffer

codec_names = {
  0: 'UNCOMPRESSED',
  1: 'SNAPPY',
  2: 'GZIP',
  3: 'LZO',
  4: 'BROTLI',
  5: 'LZ4',
  6: 'ZSTD'
}

data_type_names = {
  0: 'BOOLEAN',
  1: 'INT32',
  2: 'INT64',
  3: 'INT96',
  4: 'FLOAT',
  5: 'DOUBLE',
  6: 'BYTE_ARRAY',
  7: 'FIXED_LEN_BYTE_ARRAY'
}

encodings = {
  0: 'PLAIN',
  1: 'PLAIN_DICTIONARY',
  2: 'RLE',
  3: 'BIT_PACKED',
  4: 'DELTA_BINARY_PACKED',
  5: 'DELTA_LENGTH_BYTE_ARRAY',
  6: 'DELTA_BYTE_ARRAY',
  7: 'RLE_DICTIONARY'
}

def is_valid_parquet(path):
    with open(path, 'rb') as f:
        f.seek(-4, 2)  # Move to 4 bytes from the file end
        return f.read() == b'PAR1'

def get_metadata_length(path):
    with open(path, 'rb') as f:
         f.seek(-8, 2)  # Move to 8 bytes from the file end
         return int.from_bytes(f.read(4), byteorder='little')

def get_metadata_buffer(path, file_meta_length=None):
    if (file_meta_length is None):
        file_meta_length = get_metadata_length(path)

    with open(path, 'rb') as f:
         f.seek(-8-file_meta_length, 2)
         return f.read(file_meta_length)

def get_metadata(path, file_meta_length=None):
    buffer = get_metadata_buffer(path, file_meta_length)
    return from_buffer(buffer, "FileMetaData")

def get_row_group_length(metadata):
    return len(metadata.row_groups)

def get_row_group_info(path, row_group_index, metadata=None):
    if (metadata is None):
        metadata = get_metadata(path)

    if (row_group_index >= get_row_group_length(metadata)):
        return None

    row_group = metadata.row_groups[row_group_index]

    with open(path, 'rb') as f:
        print("Extracting information for row group", row_group_index)
        print('Num Rows', row_group.num_rows)
        print('Num Columns', len(row_group.columns))
        print('Total Byte Size', row_group.total_byte_size)

        for column in row_group.columns:
            print('-----------------------------------')
            print('Column', '.'.join(column.meta_data.path_in_schema))
            print('Data Type', data_type_names[column.meta_data.type])
            print('Num Values', column.meta_data.num_values)
            print('Codec', codec_names[column.meta_data.codec])
            print('Encodings', [ encodings[x] for x in column.meta_data.encodings ])
            print('Compressed Size', column.meta_data.total_compressed_size)
            print('Unompressed Size', column.meta_data.total_uncompressed_size)

            f.seek(column.meta_data.data_page_offset)
            if (column.meta_data.codec == 0):
               value = f.read(column.meta_data.total_uncompressed_size)
               print('Value', value)

if __name__ == "__main__":
    import sys
    if (len(sys.argv) < 2):
        print('Usage: python3 LowLevel.py <parquet file>')
        sys.exit(1)

    path = sys.argv[1]
    if (not is_valid_parquet(path)):
        print('Invalid parquet file')
        sys.exit(1)

    metadata = get_metadata(path)
    print('Version', metadata.version)
    print('Num Row Groups', get_row_group_length(metadata))

    for i in range(get_row_group_length(metadata)):
        get_row_group_info(path, i, metadata)