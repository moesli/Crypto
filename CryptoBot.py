import asyncio

import pandas as pd
import os
from binance.client import Client
from sqlalchemy import create_engine

# load api_key and api_secret from a file
with open('secret.cfg', 'r') as f:
    test_api_key = f.readline().strip()
    test_api_secret = f.readline().strip()

credientials_path = 'cryptotable-374610-a6660cf7172b.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credientials_path

client = Client(test_api_key, test_api_secret, testnet=True)
engine = create_engine('sqlite:///BTCUSDTstream.db')
engine2 = create_engine('sqlite:///BTCUSDTprofit.db')

table_id_stream = 'cryptotable-374610.cryptobot.BTCUSDTstream'
table_id_profit = 'cryptotable-374610.cryptobot.BTCUSDTprofit'

# Trendfollowing
# if crypto was rising by x % -> Buy
# if crypto was falling by x % -> Sell

async def strategy(entry, lookback, qty, open_position=False, orderPrices = []):
    while True:
        df = pd.read_sql('BTCUSDT', engine)
        if not open_position:
            lookback_df = df.iloc[-lookback:]
            cumret = (lookback_df.price.pct_change() + 1).cumprod() - 1
            if cumret[cumret.last_valid_index()] > entry:
                order = client.create_order(symbol='BTCUSDT', side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET,
                                            quantity=qty)

                orderPrices.append(float(order['cummulativeQuoteQty']))
                open_position = True
        if open_position:
            price = float(order['fills'][0]['price'])
            TSL = round(price * 0.99, 2)
            TTP = round(price * 1.001, 2)
            dfprice = df.iloc[-1].price
            if dfprice <= TSL or dfprice >= TTP:
                order = client.create_order(symbol='BTCUSDT', side=Client.SIDE_SELL,
                                            type=Client.ORDER_TYPE_MARKET, quantity=qty)
                time = pd.to_datetime(order['transactTime'], unit='ms')
                sell = float(order['cummulativeQuoteQty'])
                buy = orderPrices.pop()
                result = sell-buy
                open_position = False
                dfr = pd.DataFrame([{'Einkauf': buy, 'Verkauf': sell, 'Profit': result, 'Time': time}])
                dfr.Einkauf = dfr.Einkauf.astype(float)
                dfr.Verkauf = dfr.Verkauf.astype(float)
                dfr.Profit = dfr.Profit.astype(float)
                dfr.to_sql('BTCUSDT', engine2, if_exists='append', index=False)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(strategy(0.0001, 50, 0.01))
