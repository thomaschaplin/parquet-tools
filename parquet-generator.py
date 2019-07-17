from datetime import datetime
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

file_compression = 'GZIP'  # 'NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD'
file_name = str(uuid.uuid4()) + '_' + file_compression + '.parquet'


def write_parquet_file(compression):
    df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                       'two': ['foo', 'bar', 'baz'],
                       'three': [True, False, True]})
    table = pa.Table.from_pandas(df)
    start_time = datetime.now()
    pq.write_table(table, file_name, compression=compression)
    write_time = datetime.now() - start_time
    print('Written the parquet file', file_name, 'and it took:', write_time)


def get_parquet_file_data(columns):
    table = pq.read_table(file_name)
    table.to_pandas()
    return pq.read_table(file_name, columns=columns).to_pandas()


def get_parquet_file_metadata():
    parquet_file = pq.ParquetFile(file_name)
    return parquet_file.metadata


def get_parquet_file_schema():
    parquet_file = pq.ParquetFile(file_name)
    return parquet_file.schema


if __name__ == "__main__":
    write_parquet_file(file_compression)

    data = get_parquet_file_data(['one', 'two', 'three'])
    metadata = get_parquet_file_metadata()
    schema = get_parquet_file_schema()

    print(data)
    print(metadata)
    print(schema)
