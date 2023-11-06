###########################################################################################################################################
# BINANCE ENDPOINT CLASS
###########################################################################################################################################

import sqlite3 as lite
from binance.client import Client, AsyncClient
import asyncio

import datetime as dt
from utils import *
import time


class Binance:

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = Client(api_key=api_key, api_secret=api_secret)

    def get_data(self,
                db,
                pair_list,
                interval='8h',
                start_date=None):

        async def get_bnc_data(db, pair, start_date, interval, api_key, api_secret):
            """
            Info:   Get data kline data from binance api
            Path:   NA
            Input:  db         - string = database name
                    pair       - string = pair name
                    start_date - string = date to start getting data (YYYYMMDD)
                        Default value '20160101'
                    interval   - string = kline interval
                    api_key    - string = api key
                    api_secret - string = api secret
            Output: Data into pair table name with price data in input db with
            Note:   interval parameter
                    KLINE_INTERVAL_1MINUTE  = '1m'
                    KLINE_INTERVAL_3MINUTE  = '3m'
                    KLINE_INTERVAL_5MINUTE  = '5m'
                    KLINE_INTERVAL_15MINUTE = '15m'
                    KLINE_INTERVAL_30MINUTE = '30m'
                    KLINE_INTERVAL_1HOUR    = '1h'
                    KLINE_INTERVAL_2HOUR    = '2h'
                    KLINE_INTERVAL_4HOUR    = '4h'
                    KLINE_INTERVAL_6HOUR    = '6h'
                    KLINE_INTERVAL_8HOUR    = '8h'
                    KLINE_INTERVAL_12HOUR   = '12h'
                    KLINE_INTERVAL_1DAY     = '1d'
                    KLINE_INTERVAL_3DAY     = '3d'
                    KLINE_INTERVAL_1WEEK    = '1w'
                    KLINE_INTERVAL_1MONTH   = '1M'
            """

            end_date = dt.datetime.utcnow()

            if start_date is None:
                if interval == '8h':
                    latest_date = get_date(db, pair, '')

                    if latest_date is not False:
                        start_date = latest_date + dt.timedelta(hours=8 - 7)
                    else:
                        start_date = dt.datetime.strptime(
                            '2017-01-01', '%Y-%m-%d')
                else:
                    start_date = dt.datetime.strptime(
                        '2017-01-01', '%Y-%m-%d')

            if start_date < end_date:
                con = lite.connect(db)
                client = await AsyncClient.create(api_key, api_secret)
                try:
                    klines = await client.get_historical_klines(
                        pair, interval, start_date.strftime("%d %b %Y %H:%M:%S"),
                        end_date.strftime("%d %b %Y %H:%M:%S"), 1000)
                    await client.close_connection()
                    data = pd.DataFrame(klines,
                                        columns={
                                            'timestamp': 'datetime',
                                            'open': 'float',
                                            'high': 'float',
                                            'low': 'float',
                                            'close': 'float',
                                            'volume': 'float',
                                            'close_time': 'datetime',
                                            'quote_av': 'float',
                                            'trades': 'int',
                                            'tb_base_av': 'float',
                                            'tb_quote_av': 'float',
                                            'ignore': 'int'
                                        })
                    data['timestamp'] = pd.to_datetime(
                        data['timestamp'], unit='ms') + dt.timedelta(hours=7)

                    data = data.iloc[:-1].copy()

                    data.set_index('timestamp', inplace=True)
                    data.to_sql(pair, con, if_exists='append')
                    con.close()
                except:
                    await client.close_connection()

        loop = asyncio.get_event_loop()

        async def request(loop):
            futures = []

            for pair in pair_list:
                futures.append(
                    loop.create_task(
                        get_bnc_data(db=db,
                                    pair=pair,
                                    start_date=start_date,
                                    interval=interval,
                                    api_key=self.api_key,
                                    api_secret=self.api_secret)))
            retry = 0
            while retry < 10:
                try:
                    await asyncio.gather(*futures)
                    break
                except asyncio.exceptions.TimeoutError:
                    retry += 1
                    continue

        loop.run_until_complete(request(loop))


    def get_asset(self, asset):
        """
        Info:   Get the quantity of the asset
        Path:   NA
        Input:  asset      - string = asset name
                api_key    - string = api key
                api_secret - string = api secret
        Output: float               = total asset amount
        """

        total_asset = 0

        # info = False
        # while not info:
        #     try:

        # except requests.exceptions.ReadTimeout as e:
        #     time.sleep(1)

        info = self.client.get_account()

        for i in info['balances']:
            if float(i['free']) != 0:
                if i['asset'] == asset:
                    total_asset += float(i['free'])

        return total_asset


    def create_order(self, pair, side, amt):
        """
        Info:   Sell or buy pair on Binance according to defined amount
        Path:   NA
        Input:  pair        - string = pair to make the transaction
                side        - string = BUY/SELL
                amt         - float  = the amount of asset we will use for the transaction
                api_key     - string = api key
                api_secret  - string = api secret
        Output: order       - dict   = order response information
        """

        # Send the order
        if side == 'BUY':
            order = self.client.create_order(symbol=pair,
                                        side=side,
                                        type='MARKET',
                                        quoteOrderQty=amt,
                                        newOrderRespType='FULL')
        elif side == 'SELL':
            order = self.client.create_order(symbol=pair,
                                        side=side,
                                        type='MARKET',
                                        quantity=amt,
                                        newOrderRespType='FULL')

        return order


    def get_bnc_data(self, db, pair, start_date, interval):
        """
        Info:   Get data kline data from binance api
        Path:   NA
        Input:  db         - string = database name
                pair       - string = pair name
                start_date - string = date to start getting data (YYYYMMDD)
                    Default value '20160101'
                interval   - string = kline interval
                api_key    - string = api key
                api_secret - string = api secret
        Output: Data into pair table name with price data in input db with
        Note:   interval parameter
                KLINE_INTERVAL_1MINUTE  = '1m'
                KLINE_INTERVAL_3MINUTE  = '3m'
                KLINE_INTERVAL_5MINUTE  = '5m'
                KLINE_INTERVAL_15MINUTE = '15m'
                KLINE_INTERVAL_30MINUTE = '30m'
                KLINE_INTERVAL_1HOUR    = '1h'
                KLINE_INTERVAL_2HOUR    = '2h'
                KLINE_INTERVAL_4HOUR    = '4h'
                KLINE_INTERVAL_6HOUR    = '6h'
                KLINE_INTERVAL_8HOUR    = '8h'
                KLINE_INTERVAL_12HOUR   = '12h'
                KLINE_INTERVAL_1DAY     = '1d'
                KLINE_INTERVAL_3DAY     = '3d'
                KLINE_INTERVAL_1WEEK    = '1w'
                KLINE_INTERVAL_1MONTH   = '1M'
        """

        try:
            start_date = dt.datetime.strptime(start_date, '%Y%m%d')
        except:
            start_date = dt.datetime.strptime('20160101', '%Y%m%d')
        end_date = dt.datetime.utcnow()

        flag = False
        while not flag:
            try:
                con = lite.connect(db)

                latest_date = get_date(db, pair, '')

                if latest_date is not False:
                    start_date = latest_date + dt.timedelta(hours=8 - 7)

                if start_date < end_date:
                    log('log_master_code.log', 'Working on {0}...'.format(pair))

                    klines = self.client.get_historical_klines(
                        pair, interval, start_date.strftime("%d %b %Y %H:%M:%S"),
                        end_date.strftime("%d %b %Y %H:%M:%S"), 1000)
                    data = pd.DataFrame(klines,
                                        columns={
                                            'timestamp': 'datetime',
                                            'open': 'float',
                                            'high': 'float',
                                            'low': 'float',
                                            'close': 'float',
                                            'volume': 'float',
                                            'close_time': 'int',
                                            'quote_av': 'float',
                                            'trades': 'int',
                                            'tb_base_av': 'float',
                                            'tb_quote_av': 'float',
                                            'ignore': 'int'
                                        })
                    data['timestamp'] = pd.to_datetime(
                        data['timestamp'], unit='ms') + dt.timedelta(hours=7)

                    log('test_date.log', f"{pair} - {data.iloc[-1]}")
                    data = data.loc[data.timestamp != get_latest_txn_time(
                        is_lag=False)].copy()

                    data.set_index('timestamp', inplace=True)
                    data.to_sql(pair, con, if_exists='append')
                    con.close()
                    log('log_master_code.log', 'Finished!')

                flag = True
            except Exception as e:
                log('log_master_code.log', str(pair) + ' ' + str(e))
                print(e)
                time.sleep(5)


    def get_bnc_txn_info(self, db, bnc_txn_tbl, pair):
        """
        Info:   Get transaction information from binance (for commision tracking)
        Path:   NA
        Input:  db           - string = database name
                bnc_txn_tbl  - string = binance transaction table name
                pair         - string = pair name
                api_key      - string = api key
                api_secret   - string = api secret
        Output: SQLite table          = transactions data from binance for the input pair to input db at bnc_txn_tbl tbl
        """

        con = lite.connect(db)

        update_date = '20200101'

        if is_tbl_exist(bnc_txn_tbl, db):
            df = pd.read_sql(
                'select date(max(timestamp)) as timestamp from {0}'.format(
                    bnc_txn_tbl), con)

            if not pd.isnull(df.iloc[-1]['timestamp']):
                update_date = df.iloc[-1]['timestamp']
                query = """delete
                    from    {0}
                    where   date(timestamp)='{1}'
                            and pair='{2}'""".format(bnc_txn_tbl, update_date,
                                                    pair)

                con.execute(query)
                con.commit()
            update_date = update_date.replace('-', '')

        txn_info = pd.DataFrame(
            columns={
                'commission': 'float64',
                'commissionAsset': 'object',
                'id': 'int64',
                'isBestMatch': 'int64',
                'isBuyer': 'int64',
                'isMaker': 'int64',
                'orderId': 'int64',
                'orderListId': 'int64',
                'price': 'float64',
                'qty': 'float64',
                'quoteQty': 'float64',
                'symbol': 'object',
                'time': 'int64'
            })

        trades = self.client.get_my_trades(symbol=pair)

        for i in trades:
            txn_info = txn_info.append(i, ignore_index=True)

        txn_info['timestamp'] = pd.to_datetime(
            txn_info['time'], unit='ms') + dt.timedelta(hours=7)
        txn_info = txn_info.drop(columns='time')
        txn_info.set_index('timestamp', inplace=True)

        txn_info = txn_info.rename(
            columns={
                'commissionAsset': 'commission_asset',
                'isBestMatch': 'is_best_match',
                'isBuyer': 'is_buyer',
                'isMaker': 'is_maker',
                'orderId': 'order_id',
                'orderListId': 'order_list_id',
                'quoteQty': 'quote_qty',
                'symbol': 'pair'
            })

        txn_info.query('timestamp>={0}'.format(update_date)).to_sql(
            bnc_txn_tbl, con, if_exists='append')
        con.close()


    def gen_txn(self, db, master_tbl, signal_tbl, txn_tbl, map_tbl, txn_time):
        """
        Info:   Generate transaction information according to portfolio config from signal
        Path:   NA
        Input:  db           - string = database name
                master_tbl   - string = master table name
                signal_tbl   - string = signal table name
                txn_tbl      - string = transaction table name
                map_tbl      - string = map table name
                api_key      - string = api key
                api_secret   - string = api secret
        Output: Executing binance order according to signal and portfolio config
                Log the sucess orders
        """

        # Declare connection to SQLite
        con = lite.connect(db)

        # Get latest trading signals
        signal_list = pd.read_sql(
            """select * from {0} where timestamp = '{1}'""".format(
                signal_tbl, txn_time), con)

        if len(signal_list) != 0:

            # Update system time
            if platform == "win32":
                sync_time()

            # Get holding list
            hold_list = pd.read_sql(
                """select pair, strategy, qty from {0} where is_sold=0""".format(
                    txn_tbl), con)

            # Get sell list in the holding list
            sell_list = pd.merge(hold_list[['pair', 'qty']],\
                                signal_list.loc[signal_list.action.isin(['SELL', 'CUT_SELL']), ['pair', 'strategy', 'action']],\
                                how='inner',\
                                left_on=['pair'],\
                                right_on=['pair'])

            ## Execute selling signals

            # Iteratively execute selling signals
            for i, row in sell_list.iterrows():
                try:
                    # Get transaction amount
                    dec_num = get_min_txn(db, map_tbl, row['pair'])
                    qty = round(
                        self.get_asset(row['pair'].replace('USDT', '')) - 0.5 / 10**dec_num, dec_num)
                    if  (qty>float(row['qty'])+2/10**dec_num)\
                        or (row['pair']=='BNBUSDT' and qty>float(row['qty'])):
                        qty = row['qty']

                    # Send order to Binance
                    order = self.create_order(row['pair'], 'SELL', qty)

                    # Log and get transaction information
                    log_order(db, txn_tbl, order, row['action'], row['strategy'],
                            txn_time)
                    self.get_bnc_txn_info(db, 'bnc_' + txn_tbl, row['pair'])
                except Exception as e:
                    log('log_master_code.log',
                        f"SELL - {row['pair']} - {qty} - {e}")

            if len(signal_list.loc[signal_list.action == 'BUY']) != 0:

                ## Execute buying signals

                # Distribute transaction amount according to portfolio config
                txn_amt = round(
                    self.get_asset('USDT') /
                    (count_signal(db, master_tbl) - len(hold_list) +
                    len(sell_list)), 3)
                log(
                    'log_master_code.log',
                    f"BUY\n - Total: {count_signal(db, master_tbl)}\n - Hold: {len(hold_list)}\n - Sell: {len(sell_list)}\n"
                )
                for i, row in signal_list.loc[signal_list.action ==
                                            'BUY'].iterrows():
                    try:
                        # Send order to Binance
                        order = self.create_order(row['pair'], 'BUY', txn_amt)

                        # Log and get transaction information
                        log_order(db, txn_tbl, order, row['action'],
                                row['strategy'], txn_time)
                        self.get_bnc_txn_info(db, 'bnc_' + txn_tbl, row['pair'])
                    except Exception as e:
                        log('log_master_code.log',
                            f"BUY - {row['pair']} - {txn_amt} - {e}")
