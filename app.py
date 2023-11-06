import os
import glob
from dask import dataframe as dd


def main():
    print('File format conversion started')
    src_dir = os.environ['SRC_DIR']
    #tgt_dir = os.environ['TGT_DIR']
    src_file_names = sorted(glob.glob(f'{src_dir}/NYSE*.txt.gz'))
    tgt_file_names = [
        file.replace('txt', 'json').replace('nyse_data', 'nyse_json') 
        for file in src_file_names
    ]
    df = dd.read_csv(
        src_file_names,
        names=['ticker', 'trade_date', 'open_price', 'low_price',
               'high_price', 'close_price', 'volume'],
        blocksize=None
    )
    print('Dataframe is created and will be writen in json format')
    df.to_json(
        tgt_file_names,
        orient='records',
        lines=True,
        compression='gzip'
    )
    print('File format conversion completed')


if __name__ == '__main__':
    main()