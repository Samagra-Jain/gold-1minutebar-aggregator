import pandas as pd
import sqlite3
import warnings
import ssl
import websockets
import json
import time
from datetime import datetime, timedelta
import asyncio

warnings.filterwarnings('ignore')

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def get_instruments(file):
    with open(file, 'r') as f:
        instruments = [line.strip() for line in f.readlines()]
    return instruments


def get_tender_dates(tender_file):
    tender_dates = {}
    rollovers = {}
    with open(tender_file, 'r') as f:
        for line in f:
            if line.strip():
                contract, date = line.split('=')
                tender_dates[contract.strip()] = datetime.strptime(date.strip(), '%d-%m-%Y').date()
                rollovers[contract.strip()] = False
    return tender_dates, rollovers


def check_for_tender_rollover(rollovers, instrument, tender_dates):
    today = datetime.today().date()
    if today > tender_dates[instrument]:
        rollovers[instrument] = True
    else:
        rollovers[instrument] = False
    return rollovers


def get_gold_futures(rollovers, data_file, instruments_file, number_of_expiries):
    instruments = get_instruments(instruments_file)
    df = pd.read_csv(data_file)
    instrument_dicts = {}
    for instrument in instruments:
        df_instrument = df[
            (df['exchange1'] == "MCX") &
            (df['symbol'] == instrument) &
            (df['instrument_type'] == 'FUTCOM')]
        df_instrument['expiry_date'] = pd.to_datetime(df_instrument['expiry_date'])
        expiry_list = sorted(df_instrument['expiry_date'].unique())
        base_dict = {
            i + 1: (row.security_id, instrument, row.expiry_date, row.exchange1)
            for i, row in enumerate(df_instrument.sort_values(by='expiry_date').itertuples())}
        updated_base_dict = {}
        expiry_offset = 0
        for expiry_num, instrument_details in base_dict.items():
            instrument_code = f"{instrument}{expiry_num}"
            if rollovers.get(instrument_code, False):
                expiry_offset += 1
            else:
                updated_base_dict[expiry_num - expiry_offset] = instrument_details
        updated_base_dict = {
            k: v for k, v in sorted(updated_base_dict.items())[:number_of_expiries]}
        instrument_dicts[instrument] = updated_base_dict
    return instrument_dicts


def create_tables(instrument_dicts, cur):
    sec_id_dict = {}
    ohlc_dict = {}
    for instrument, detail_dict in instrument_dicts.items():
        for expiry_number, details in detail_dict.items():
            table_name = f'{instrument}{expiry_number}'
            sec_id_dict[table_name] = (details[0], details[3], details[2])
            ohlc_dict[table_name] = []
            query = f'''
            CREATE TABLE IF NOT EXISTS {table_name}(
                timestamp TIMESTAMP NOT NULL,
                expiry_date DATE NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL
            )'''
            cur.execute(query)
    return sec_id_dict, ohlc_dict


def create_temp_db(instrument_dicts):
    date_str = datetime.now().strftime('%Y-%m-%d')
    temp_db_name = f"temp_{date_str}.db"
    conn_temp = sqlite3.connect(temp_db_name)
    cur_temp = conn_temp.cursor()
    sec_id_dict, ohlc_dict = create_tables(instrument_dicts, cur_temp) 
    conn_temp.commit()
    return conn_temp, cur_temp, temp_db_name, sec_id_dict, ohlc_dict


def push_temp_to_main(temp_db_name, conn_main, cur_main):
    conn_temp = sqlite3.connect(temp_db_name)
    cur_temp = conn_temp.cursor()

    query = "SELECT name FROM sqlite_master WHERE type='table';"
    cur_temp.execute(query)
    tables = [row[0] for row in cur_temp.fetchall()]

    for table in tables:
        query = f"SELECT * FROM {table}"
        rows = cur_temp.execute(query).fetchall()

        for row in rows:
            insert_query = f'''
            INSERT INTO {table} (timestamp, expiry_date, open, high, low, close)
            VALUES (?, ?, ?, ?, ?, ?)
            '''
            cur_main.execute(insert_query, row)

    conn_main.commit()
    conn_temp.close()


def delete_temp_db(temp_db_name):
    import os
    if os.path.exists(temp_db_name):
        os.remove(temp_db_name)


async def get_ltp(sec_id_dict, ohlc_dict):
    async with websockets.connect('MULTITRADE CREDS', ssl=ssl_context) as websocket:
        response = await websocket.recv()
        if "HandShake" in response:
            for instrument, security_id_exchange in sec_id_dict.items():
                sc = json.dumps({
                    "Message": "LTP",
                    "EXC": security_id_exchange[1],
                    "SECID": security_id_exchange[0]
                })
                await websocket.send(sc)
                while True:
                    response = await websocket.recv()
                    try:
                        data = json.loads(response)
                        if "LTP" in data and data.get("SECID") == security_id_exchange[0]:
                            ltp_entry = {
                                'timestamp': datetime.now(),
                                'ltp': float(data.get("LTP")),
                                'expiry': security_id_exchange[2]
                            }
                            ohlc_dict[instrument].append(ltp_entry)
                            break
                    except json.JSONDecodeError:
                        print(f"Invalid JSON received: {response}")
                        continue
        else:
            print("WebSocket connection failed")
    return ohlc_dict


def aggregate_ltp_bars(ohlc_dict):
    aggregated_bars = {}
    for instrument, ltp_entries in ohlc_dict.items():
        if not ltp_entries:
            continue
        open_price = ltp_entries[0]['ltp']
        high_price = max(entry['ltp'] for entry in ltp_entries)
        low_price = min(entry['ltp'] for entry in ltp_entries)
        close_price = ltp_entries[-1]['ltp']
        expiry_date = ltp_entries[0]['expiry']
        timestamp = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)

        aggregated_bars[instrument] = {
            'timestamp': timestamp,
            'expiry_date': expiry_date,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price
        }
    return aggregated_bars


def push_to_db(aggregated_bars, cur, conn):
    for instrument, bar in aggregated_bars.items():
        query = f'''
        INSERT INTO {instrument} (timestamp, expiry_date, open, high, low, close)
        VALUES (?, ?, ?, ?, ?, ?)
        '''
        expiry_date = bar['expiry_date'].date() if isinstance(bar['expiry_date'], pd.Timestamp) else bar['expiry_date']
        cur.execute(query, (
            bar['timestamp'],
            expiry_date,
            bar['open'],
            bar['high'],
            bar['low'],
            bar['close']
        ))
    conn.commit()


async def main():
    tender_file = 'tender_dates.txt'
    instruments_file = 'name_file.txt'
    data_file = 'latest_instruments.txt'
    number_of_expiries = 2

    tender_dates, rollovers = get_tender_dates(tender_file)
    for instrument in tender_dates.keys():
        rollovers = check_for_tender_rollover(rollovers, instrument, tender_dates)

    instrument_dicts = get_gold_futures(rollovers, data_file, instruments_file, number_of_expiries)

    conn_main = sqlite3.connect('gold_data.db')
    cur_main = conn_main.cursor()

    conn_temp, cur_temp, temp_db_name, sec_id_dict, ohlc_dict = create_temp_db(instrument_dicts)

    print("Starting data collection...")

    while True:
        now = datetime.now()
        start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        end_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        end_of_day = now.replace(hour=23, minute=56, second=0, microsecond=0)

        if start_time.time() <= now.time() <= end_time.time():
            start_of_minute = now
            while (datetime.now() - start_of_minute).seconds < 59:
                try:
                    ohlc_dict = await get_ltp(sec_id_dict, ohlc_dict)
                except Exception as e:
                    print(f"Error during LTP fetch: {e}")
            aggregated_bars = aggregate_ltp_bars(ohlc_dict)
            print(f"Aggregated Bars at {datetime.now()}: {aggregated_bars}")
            try:
                push_to_db(aggregated_bars, cur_temp, conn_temp)
                print(f"Inserted bars into temporary DB at {datetime.now()}")
            except Exception as e:
                print(f"Error pushing to temporary DB: {e}")
            ohlc_dict = {key: [] for key in ohlc_dict.keys()}
            time.sleep(1)

        elif now.time() >= end_of_day.time():
            print("End of trading day. Consolidating data.")
            push_temp_to_main(temp_db_name, conn_main, cur_main)
            delete_temp_db(temp_db_name)
            conn_temp, cur_temp, temp_db_name, sec_id_dict, ohlc_dict = create_temp_db(instrument_dicts)
            time.sleep(60)
        else:
            print("Outside trading hours. Sleeping...")
            time.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())