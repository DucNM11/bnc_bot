import pandas as pd
from sqlite3 import connect
import time
from datetime import datetime as dt
import utils as lib
from binance import Binance
import configparser


def main():

    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('config.ini')

    # YOUR API KEYS HERE
    api_key = config.get('binance', 'api_key')
    api_secret = config.get('binance', 'api_secret')

    receiver = config.get('incident', 'email')
    db = config.get('db', 'db_name')
    log_file = 'log_master_code.log'
    map_tbl = 'master_tbl'
    bnc = Binance(api_key=api_key, api_secret=api_secret)

    try:
        pair_list = pd.read_sql(f'select distinct pair from {map_tbl}',
                                connect(db))['pair'].to_list()
        master_tbl = 'master_tbl'
        signal_tbl = 'signal_tbl'
        txn_tbl = 'txn_tbl'
        if not lib.is_tbl_exist(tbl_name=txn_tbl, db=db):
            df = lib.ini_txn_tbl()
            df.to_sql(txn_tbl, con)

        txn_time = lib.get_latest_txn_time()
    except Exception as err:
        with open(log_file, 'a+') as f:
            f.write(f'{dt.utcnow()} - {e}\n')
        lib.send_err(str(err), 'PARAMETERS')

    try:
        lib.log(log_file, f'Transaction time: {txn_time}')

        start = time.time()
        # Updating data
        num_of_complete = 0
        retry = 0

        while num_of_complete != len(pair_list):
            if retry == 3:
                lib.log(log_file, f'Warning - completeness: {num_of_complete}')
                time.sleep(20)
                retry = 0

            bnc.get_data(db=db,
                        pair_list=pair_list)
            num_of_complete = lib.check_completeness(db=db,
                                                    pair_list=pair_list,
                                                    latest_txn_time=txn_time)

            retry += 1

        lib.log(log_file, f'Getting data: {time.time() - start}')

        start = time.time()
        # Generating signal from data
        lib.gen_ema_signal(db, master_tbl, None)
        lib.log(log_file, f'Generating signals: {time.time() - start}')

        start = time.time()
        # Generating transactions + log transactions
        bnc.gen_txn(db, master_tbl, signal_tbl, txn_tbl, map_tbl, txn_time)
        lib.log(log_file, f'Doing transactions: {time.time() - start}')

        # Generating email report
        lib.gen_mail(db,
                    txn_tbl,
                    signal_tbl,
                    receiver,
                    data_quality=num_of_complete,
                    txn_time=txn_time)

    except Exception as err:
        with open(log_file, 'a+') as f:
            f.write(f'{dt.utcnow()} - {e}\n')
        lib.send_err(str(err), txn_time)

if __name__ == "__main__":
    main()