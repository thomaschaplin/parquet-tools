import pandas
import pyarrow.parquet as pq

if __name__ == "__main__":
    df = pq.read_pandas('data.snappy.parquet')
    data = pq.read_table('data.snappy.parquet')
    pq.write_to_dataset(data, root_path='dataset', partition_cols=['FIRSTNAME'])