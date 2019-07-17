import pandas
import pyarrow.parquet as pq

if __name__ == "__main__":
    df = pandas.read_csv('data.csv')
    df.to_parquet('data.snappy.parquet', engine='auto',
                  compression='SNAPPY')  # 'NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD'
    data = pq.read_pandas('data.snappy.parquet').to_pandas()
    print(data)
