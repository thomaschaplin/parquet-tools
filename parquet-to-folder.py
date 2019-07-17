import pandas
import pyarrow.parquet as pq

if __name__ == "__main__":
    # Write parquet file to a dataset
    df = pq.read_pandas('data.snappy.parquet')
    data = pq.read_table('data.snappy.parquet')
    pq.write_to_dataset(data, root_path='dataset', partition_cols=['FIRSTNAME'])

    # Read parquet files from a dataset
    dataset = pq.ParquetDataset('dataset/')
    data_from_dataset = dataset.read().to_pandas()
    print(data_from_dataset)
