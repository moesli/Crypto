import asyncio

import pandas as pd
from binance import BinanceSocketManager
from binance.client import Client, AsyncClient
from sqlalchemy import create_engine


async def main():
    # load api_key and api_secret from a file
    with open('secret.cfg', 'r') as f:
        test_api_key = f.readline().strip()
        test_api_secret = f.readline().strip()

    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    ts = bm.trade_socket('BTCUSDT')
    engine = create_engine('sqlite:///BTCUSDTstream.db')
    # then start receiving messages
    async with ts as tscm:
        i = 0
        while True:
            msg = await tscm.recv()
            if msg:
                frame = create_frame(msg)
                frame.to_sql('BTCUSDT', engine, if_exists='append', index=False)
                if i == 100:
                    #print(frame)
                    df = pd.read_sql('BTCUSDT', engine)
                    if len(df) > 10000:
                        con = engine.connect()
                        con.execute('DELETE FROM BTCUSDT WHERE rowid IN (SELECT rowid FROM BTCUSDT LIMIT 9000)')
                        con.close()
                    i = 0
                i += 1

def create_frame(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'p', 'T']]
    df.columns = ['symbol', 'price', 'time']
    df.price = df.price.astype(float)
    df.time = pd.to_datetime(df.time, unit='ms')
    return df


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
