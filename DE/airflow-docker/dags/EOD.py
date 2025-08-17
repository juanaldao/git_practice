# dags/EOD.py

import pandas as pd
import requests
from sqlalchemy import create_engine

def run():
    # Your API key
    API_KEY = '677856c597b298.10850742'

    # Top 10 S&P 500 tickers
    tickers = {
        "AAPL.US": "Apple",
        "MSFT.US": "Microsoft",
        "NVDA.US": "Nvidia",
        "AMZN.US": "Amazon.com",
        "GOOGL.US": "Alphabet",
        "META.US": "Meta Platforms",
        "BRK-B.US": "Berkshire Hathaway",
        "AVGO.US": "Broadcom",
        "TSLA.US": "Tesla",
        "JPM.US": "JPMorgan Chase"
    }

    # Base URL for latest EOD data (1 result only)
    base_url = 'https://eodhd.com/api/eod/{}?api_token={}&fmt=json&limit=1&order=d'

    # Fetch data
    data = []
    for ticker, name in tickers.items():
        url = base_url.format(ticker, API_KEY)
        try:
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
            if json_data:
                latest = json_data[0]
                data.append({
                    "Date": latest.get("date"),
                    "Ticker": ticker,
                    "Closing_price": latest.get("close")
                })
            else:
                data.append({"Ticker": ticker, "Company": name, "Date": None, "Closing_price": None})
        except Exception as e:
            print(f"Failed to retrieve data for {ticker}: {e}")
            data.append({"Ticker": ticker, "Company": name, "Date": None, "Closing_price": None})

    # Create DataFrame
    df = pd.DataFrame(data)

    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    df.to_sql('stock_prices', engine, if_exists='replace', index=False)
