import asyncio
import pandas as pd
import os
import time
from binance.client import Client
from sqlalchemy import create_engine

async def main():
    # load api_key and api_secret from a file
   while True:
        with open('secret.cfg', 'r') as f:
            test_api_key = f.readline().strip()
            test_api_secret = f.readline().strip()

        credientials_path = 'cryptotable-374610-a6660cf7172b.json'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credientials_path
        table_id = 'cryptotable-374610.cryptobot.BTCUSDTprofit'

        # Create a client
        client = Client(test_api_key, test_api_secret, testnet=True)

        # Create an engine
        engine = create_engine('sqlite:///BTCUSDTprofit.db')

        df = pd.read_sql('BTCUSDT', engine)
        df.to_gbq(table_id, if_exists='replace', project_id='cryptotable-374610')
        time.sleep(60)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
