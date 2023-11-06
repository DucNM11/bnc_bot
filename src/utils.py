import sqlite3 as lite

import pandas as pd
import csv
import struct
from numpy import array
import datetime as dt
import configparser

from tabulate import tabulate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
import smtplib
import socket

from sys import platform
if platform == "win32":
    import win32api

###########################################################################################################################################
# UTILITIES
###########################################################################################################################################

def gen_mail(db, txn_tbl, signal_tbl, receiver, data_quality, txn_time):
    """
    Info:   Get data about recent trades and send to the destination mail
    Path:   NA
    Input:  db         - string = database name
            txn_tbl    - string = transaction table name
            signal_tbl - string = signal table name
            receiver   - string = receiver email
    Output: A mail about the recent session trades
    """

    # Get mail content
    con = lite.connect(db)

    signal = pd.read_sql(
        """select  pair,
                   strategy,
                   action
            from    {0}
            where   timestamp='{1}'""".format(
            signal_tbl, txn_time), con)

    txn = pd.read_sql(
        """select pair,
                  strategy,
                  action,
                  quote_qty,
                  pnl
           from    {0}
           where   timestamp='{1}'
           order by
               action desc""".format(txn_tbl, txn_time), con)

    rpt_tbl = pd.merge( signal,
                        txn,
                        how='left',\
                        left_on=['pair', 'strategy', 'action'],\
                        right_on=['pair', 'strategy', 'action'])

    try:
        rpt_tbl[['pnl',
                 'quote_qty']] = rpt_tbl[['pnl',
                                          'quote_qty']].astype('float64')
        rpt_tbl[['pnl', 'quote_qty']] = rpt_tbl[['pnl', 'quote_qty']].round(2)
    except Exception as e:
        pass

    rpt_tbl = rpt_tbl.sort_values(by=['action'])
    rpt_tbl.to_csv('data.csv', index=False)

    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('config.ini')

    # Send mail
    sender = config.get("incident", "sender")
    token = config.get("incident", "sender_token")
    server = config.get("incident", "server")

    text = """
    {table}"""

    html = """
    <html><body>
    {table}
    </body></html>
    """

    with open('data.csv') as input_file:
        reader = csv.reader(input_file)
        data = list(reader)

    text = text.format(
        table=tabulate(data, headers="firstrow", tablefmt="simple"))
    html = html.format(
        table=tabulate(data, headers="firstrow", tablefmt="html"))

    message = MIMEMultipart(
        "alternative", None,
        [MIMEText(text), MIMEText(html, 'html')])

    message['Subject'] = f"{txn_time} - {data_quality}"
    message['From'] = sender
    message['To'] = receiver
    server = smtplib.SMTP(server)
    server.starttls()
    server.login(sender, token)
    server.sendmail(sender, receiver, message.as_string())
    server.quit()


def send_err(err, txn_time='ERROR', receiver='potato_say@outlook.com'):
    """
    Info:   Get data about recent trades and send to the destination mail
    Path:   NA
    Input:  text       - string = text to be sent
            txn_time   - string = transaction table name
            signal_tbl - string = signal table name
            receiver   - string = receiver email
    Output: A mail about the recent session trades
    """

    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('config.ini')

    # Send mail
    sender = config.get("incident", "sender")
    token = config.get("incident", "sender_token")
    server = config.get("incident", "server")

    message = EmailMessage()
    message['Subject'] = f"{txn_time} - ERROR"
    message['From'] = sender
    message['To'] = receiver

    server = smtplib.SMTP(server)
    server.starttls()
    server.login(sender, token)
    server.sendmail(sender, receiver, str(err))
    server.quit()


def get_min_txn(db, map_tbl, pair):
    """
    Info:   Get decimal place to round the minimum amount from the transaction
    Path:   NA
    Input:  db         - string = database name
            map_tbl    - string = map table name
            pair       - string = pair to get the decimal number to round
    Output: int                 = number of decimal to round to
    """

    con = lite.connect(db)
    df = pd.read_sql(
        "select * from {0} where pair='{1}'".format(map_tbl, pair), con)
    con.close()

    num = float(df['min_txn_amt'][0])
    rs = 0
    while num < 1:
        num *= 10
        rs += 1

    return rs


def sync_time():
    """
    Info:   Update system time
    Path:   NA
    Input:  NA
    Output: Updated system time
    """

    server_list = [
        'time.windows.com', 'pool.ntp.org', 'ntp.iitb.ac.in', 'time.nist.gov'
    ]

    for addr in server_list:
        # http://code.activestate.com/recipes/117211-simple-very-sntp-client/
        epoch_time = None
        TIME1970 = 2208988800  # Thanks to F.Lundh
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data = '\x1b' + 47 * '\0'
        data = data.encode()
        try:
            # Timing out the connection after 5 seconds, if no response received
            client.settimeout(5.0)
            client.sendto(data, (addr, 123))
            data, address = client.recvfrom(1024)
            if data:
                epoch_time = struct.unpack('!12I', data)[10]
                epoch_time -= TIME1970
        except socket.timeout:
            pass

        if epoch_time is not None:
            # SetSystemTime takes time as argument in UTC time. UTC time is obtained using utcfromtimestamp()
            utcTime = dt.datetime.utcfromtimestamp(epoch_time)
            win32api.SetSystemTime(utcTime.year, utcTime.month,
                                   utcTime.weekday(), utcTime.day,
                                   utcTime.hour, utcTime.minute,
                                   utcTime.second, 0)
            # Local time is obtained using fromtimestamp()
            localTime = dt.datetime.fromtimestamp(epoch_time)
            log(
                'log_master_code.log', "Time updated to: " +
                localTime.strftime("%Y-%m-%d %H:%M") + " from " + addr)
            return True

    sync_time()


def log(file, string):
    """
    Info:   Generate/append in a log
    Path:   NA
    Input:  file       - string = output log file name
            string     - string = content to be logged
    Output: file log
    """

    log = open(file, 'a+')
    log.write('{0} - {1}\n'.format(
        str(dt.datetime.utcnow() + dt.timedelta(hours=7)), string))


def get_latest_txn_time(is_lag=True):
    """
    Info:   Generate latest trading time
    Path:   NA
    Input:  datetime
    Output: dateime = Latest trading time that smaller than input datetime
    """

    max_time = (dt.datetime.utcnow() + dt.timedelta(hours=7) - dt.timedelta(
        hours=8)) if is_lag else dt.datetime.utcnow() + dt.timedelta(hours=7)

    if max_time.hour < 7:
        return (max_time - dt.timedelta(days=1)).replace(hour=23,
                                                         minute=0,
                                                         second=0,
                                                         microsecond=0)

    txn_time = array([7, 15, 23])

    return max_time.replace(hour=txn_time[txn_time <= max_time.hour].max(),
                            minute=0,
                            second=0,
                            microsecond=0)


def normalize(df, col_name):
    """
    Info:   Generate a normalized column
    Path:   NA
    Input:  df       - dataframe = input dataframe
            col_name - string    = column needs to be normalized
    Output: dataframe            = df with col_name_n normalized
    """
    max_val = df[col_name].max()
    df[col_name + '_n'] = df[col_name] / max_val
    return df


def get_pair_list(file_name):
    """
    Info:   Get list of pair in txt file
    Path:   //data
    Input:  file_name - string = file name
    Output: list               = list of pair
    """
    file = open('data//{0}.txt'.format(file_name), 'r')
    list = file.read().splitlines()
    file.close()
    return list


def EMA(ts, timespan):
    """
    Info:   Get exponential moving average of a timeseries
    Path:   NA
    Input:  ts       - timeseries = the input timeseries
            timespan - int        = the timespan EMA will be calculated on
    Output: timeseries            = Return a series of EMA according to the timeseries
    """
    return ts.ewm(span=timespan, adjust=False).mean()


def simulate(df, ema, cut_loss):
    """
    Info:   Simulate
    Path:   NA
    Input:  df       - dataframe = dataframe that will be simulated
            ema      - int       = ema signal
            cut_loss - float     = cut loss percentage
    Output: A list               = Return a list of [pnl total (float); (dataframe) of trading according to timeseries]
    """

    df['ema'] = df['close'].ewm(span=ema, adjust=False).mean()
    df = df.dropna()

    bud = 100
    tokens = 0
    b_price = 0
    l_price = 0
    result = 100
    pflo = []

    for i, row in df.iterrows():
        if row['close'] > row['ema'] and l_price < l_ema and bud != 0:
            tokens = bud / row['close']
            tokens -= 0.002 * tokens
            b_price = row['close']
            pflo.append([row['timestamp'], None, b_price, bud * 0.998, 'BUY'])
            bud = 0

        if (row['close'] < b_price * (1 - cut_loss)) and tokens != 0:
            bud = tokens * row['close']
            bud -= 0.002 * bud
            pflo.append(
                [row['timestamp'], row['close'], b_price, bud, 'CUT_SELL'])
            # print (row['timestamp'], row['close'], b_price, bud, 'SELL')
            tokens = 0

        if row['close'] < row['ema'] and l_price > l_ema and tokens != 0:
            bud = tokens * row['close']
            bud -= 0.002 * bud
            pflo.append([row['timestamp'], row['close'], b_price, bud, 'SELL'])
            tokens = 0

        l_price = row['close']
        l_ema = row['ema']
        if tokens == 0:
            result = bud
        else:
            result = tokens * row['close']
    if len(pflo) != 0:
        df = pd.DataFrame(pflo,
                          columns={
                              'timestamp': 'datetime64',
                              'sell_price': 'float64',
                              'buy_price': 'float64',
                              'pnl': 'float64',
                              'action': 'object'
                          })
        return [result, df]
    else:
        return [result, None]


###########################################################################################################################################
# LOCAL ENDPOINT
###########################################################################################################################################

def ini_txn_tbl():
    """
Info:   Initiate txn tbl structure as a DataFrame without data
Path:   NA
Input:  NA
Output: A DataFrame with txn_tbl structure
    """
    df = pd.DataFrame(columns=[
        'timestamp', 'timestamp_txn', 'order_id', 'pair', 'strategy', 'action',
        'buy_price', 'sell_price', 'qty', 'quote_qty', 'commission',
        'commission_asset', 'quote_commision', 'is_sold', 'pnl'
    ])
    df = df.astype({
        'timestamp': 'datetime64[ns]',
        'timestamp_txn': 'datetime64[ns]',
        'order_id': 'object',
        'pair': 'object',
        'strategy': 'object',
        'action': 'object',
        'buy_price': 'float64',
        'sell_price': 'float64',
        'qty': 'float64',
        'quote_qty': 'float64',
        'commission': 'float64',
        'commission_asset': 'float64',
        'quote_commision': 'float64',
        'is_sold': 'bool',
        'pnl': 'float64'
    })

    return df


def check_completeness(db, pair_list, latest_txn_time):
    """
Description: Check the completeness of extracted price data from the source
Input:
    db              - string = database name
    pair_list       - string = master table name for pair list
    latest_txn_time - datetime = latest transaction time to check for
Output:
    Number of pair have the expected data
    """

    conn = lite.connect(db)
    df = None
    for pair in pair_list:
        tmp_df = pd.read_sql(
            f"select * from {pair} where timestamp='{latest_txn_time}'", conn)
        if df is None:
            df = tmp_df
        else:
            df = pd.concat([df, tmp_df])
    return len(df)


def get_date(db, tbl, where):
    """
    Info:   Get most recent date of a table in SQLite database
    Path:   NA
    Input:  db    - string     = database name
            tbl   - string     = table name
            where - string     = where conditions if needed
    Output: dataframe.datetime = Last update timestamp of input table
            If table does not exist or Nan value return False
    """

    con = lite.connect(db)
    if is_tbl_exist(tbl, db):
        df = pd.read_sql(
            'select max(timestamp) as timestamp from {0} {1}'.format(
                tbl, where), con)

        df = df.astype({'timestamp': 'datetime64'})

        if pd.isnull(df.iloc[-1]['timestamp']):
            return False
        else:
            return df.iloc[-1]['timestamp']
    else:
        return False


def get_pair_data(pair, db):
    """
    Info:   Get data of a pair from database
    Path:   NA
    Input:  pair    - string    = pair name
            db      - string    = database dir
    Output: df      - dataframe = dataframe with pair data
    """
    con = lite.connect(db)
    df = pd.read_sql('select timestamp, close from {0}'.format(pair), con)

    df = df.astype({'timestamp': 'datetime64', 'close': 'float64'})

    return df


def is_tbl_exist(tbl_name, db):
    """
    Info:   Check if table in SQLite server exists
    Path:   NA
    Input:  tbl_name  - string = table name
    Output: True/False
    """
    con = lite.connect(db)
    cur = con.cursor()

    cur.execute(
        """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{0}'"""
        .format(tbl_name))

    #if the count is 1, then table exists
    if cur.fetchone()[0] == 1:
        con.commit()
        con.close()
        return True

    con.commit()
    con.close()
    return False


def count_signal(db, master_tbl):
    """
    Info:   Get total number of signals from portfolio
    Path:   NA
    Input:  db         - string = database has master table
            master_tbl - string = master table stores signal
    Output: int                 = Total number of signals
    """

    con = lite.connect(db)

    count = pd.read_sql('select count(*) count from {0}'.format(master_tbl),
                        con)

    return count['count'][0]


def log_order(db, txn_tbl, order, action, strategy, timestamp):
    """
    Info:   Update order information into input database and table
    Path:   NA
    Input:  db        - string             = database name
            txn_tbl   - string             = transaction table name
            order     - dict               = order information
            action    - string             = BUY/SELL/CUT_SELL
            strategy  - string             = strategy code
            timestamp - dataframe.datetime = batch timestamp
    Output: Data into txn_tbl (SQLite table)
    """

    con = lite.connect(db)
    pnl = ini_txn_tbl()

    if action == 'BUY':
        pair = lambda x, y: x + 'USDT' if (x != 'USDT') else 'USDT'

        pnl = pnl.append(
            {
                'timestamp':
                timestamp,
                'timestamp_txn':
                order['transactTime'],
                'order_id':
                order['orderId'],
                'pair':
                order['symbol'],
                'strategy':
                strategy,
                'action':
                action,
                'buy_price':
                float(order['cummulativeQuoteQty']) /
                float(order['executedQty']),
                'sell_price':
                None,
                'qty':
                order['executedQty'],
                'quote_qty':
                order['cummulativeQuoteQty'],
                'commission':
                None,
                'commission_asset':
                None,
                'quote_commision':
                None,
                'is_sold':
                0,
                'pnl':
                0
            },
            ignore_index=True)
        # pnl['timestamp'] = pd.to_datetime(pnl['timestamp'], unit='ms') + dt.timedelta(hours=7)
        pnl['timestamp_txn'] = pd.to_datetime(
            pnl['timestamp_txn'], unit='ms') + dt.timedelta(hours=7)

        pnl.to_sql(txn_tbl, con, if_exists='append', index=False)

    elif action in ['SELL', 'CUT_SELL']:
        tmp = pd.read_sql(
            """select buy_price, quote_qty from {0} where pair='{1}' and strategy='{2}' and is_sold=0"""
            .format(txn_tbl, order['symbol'], strategy), con)

        #BACKLOG FOR COMMISSION pair = lambda x, y: x+'USDT' if (x!='USDT') else 'USDT'
        pnl = pnl.append(
            {
                'timestamp':
                timestamp,
                'timestamp_txn':
                order['transactTime'],
                'order_id':
                order['orderId'],
                'pair':
                order['symbol'],
                'strategy':
                strategy,
                'action':
                action,
                'buy_price':
                tmp['buy_price'][0],
                'sell_price':
                float(order['cummulativeQuoteQty']) /
                float(order['executedQty']),
                'qty':
                order['executedQty'],
                'quote_qty':
                order['cummulativeQuoteQty'],
                'commission':
                None,
                'commission_asset':
                None,
                'quote_commision':
                None,
                'is_sold':
                1,
                'pnl':
                (float(order['cummulativeQuoteQty']) -
                 float(tmp['quote_qty'][0])) * 100 / float(tmp['quote_qty'][0])
            },
            ignore_index=True)
        # pnl['timestamp'] = pd.to_datetime(pnl['timestamp'], unit='ms') + dt.timedelta(hours=7)
        pnl['timestamp_txn'] = pd.to_datetime(
            pnl['timestamp_txn'], unit='ms') + dt.timedelta(hours=7)

        # Update new order
        pnl.to_sql(txn_tbl, con, if_exists='append', index=False)

        # Update old order is_sold status
        cur = con.cursor()
        cur.execute(""" update  {0}
                        set     is_sold=1
                        where   pair='{1}' and strategy='{2}' and is_sold=0""".
                    format(txn_tbl, order['symbol'], strategy))
        con.commit()


def get_master_data(db, tbl):
    """
    Info:   Get master data from input master_tbl
    Path:   NA
    Input:  db  - string = database name
            tbl - string = master table name
    Output: dataframe    = Return a dataframe of pair and signal
    """

    con = lite.connect(db)

    df = pd.read_sql('select * from {0}'.format(tbl), con)

    df = df.astype({'pair': 'object', 'ema': 'int64', 'cut_loss': 'float64'})

    return df


def gen_ema_signal(db, master_tbl, start_date):
    """
    Info:   Generate signals according to master table
    Path:   NA
    Input:  db           - string  = database name
            master_tbl   - string  = master table name
            start_date   - string  = date to start generating signals (YYYYMMDD)
    Output: Data in a SQLite table = signals list in signal_tbl of the input database
    """

    # Get master table data as dataframe
    df_master = get_master_data(db, master_tbl)
    signal_tbl = 'signal_tbl'
    con = lite.connect(db)

    for i, row in df_master.iterrows():
        if  start_date is None\
            or start_date == '':
            start_date = dt.datetime.today() - dt.timedelta(
                days=row['ema'] * 3)

        df = get_pair_data(row['pair'], db)
        df = df.loc[df.timestamp > '{0}'.format(start_date)]
        result = simulate(df, row['ema'], row['cut_loss'])

        if result[1] is not None:
            # Get strategy code
            strategy = str(row['ema']) + str(row['cut_loss'])

            # Get latest signal of the pair
            latest_date = get_date(
                db, 'signal_tbl',
                """where pair='{0}' and strategy='{1}'""".format(
                    row['pair'], strategy))
            if latest_date is False:
                latest_date = start_date

            # Calculating statistic information
            result[1]['pair'] = row['pair']
            result[1]['strategy'] = strategy

            # Update signal table with the most update data
            result[1].loc[result[1].timestamp > '{0}'.format(latest_date)][[
                'timestamp', 'pair', 'strategy', 'action', 'buy_price',
                'sell_price'
            ]].to_sql(signal_tbl, con, if_exists='append', index=False)
